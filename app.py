import os
import unicodedata
import json
import pandas as pd
from io import BytesIO
from datetime import datetime, time, timedelta
from flask import (
    Flask, render_template, request, redirect,
    url_for, send_file, session
)

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev_secret_key')

# Carpetas de entrada/salida
UPLOAD_FOLDER = os.path.abspath(os.path.dirname(__file__))
OUTPUT_FOLDER = os.path.join(UPLOAD_FOLDER, 'outputs')
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Constantes de negocio
IDEAL_PER_LEADER = 21    # ratio objetivo
ASSIGN_WINDOW    = 3     # ventana en horas para asignar reps

def to_time(val):
    """Convierte distintos formatos de 'INGRESO' a datetime.time."""
    if pd.isna(val): return None
    if isinstance(val, time): return val
    if isinstance(val, datetime): return val.time()
    if isinstance(val, str):
        for fmt in ('%H:%M:%S','%H:%M'):
            try: return datetime.strptime(val.strip(), fmt).time()
            except: pass
    if isinstance(val, (int, float)):
        try:
            base = datetime(1899,12,30)
            return (base + timedelta(days=float(val))).time()
        except: pass
    return None

def normalize(text):
    s = str(text).strip().lower()
    return unicodedata.normalize('NFKD', s)

@app.route('/', methods=['GET','POST'])
def upload():
    if request.method=='POST':
        f = request.files.get('file')
        if not f:
            return render_template('upload.html', error='Debes subir un archivo')
        fn = f.filename
        f.save(os.path.join(UPLOAD_FOLDER, fn))
        return redirect(url_for('select', filename=fn))
    return render_template('upload.html')

@app.route('/select/<filename>', methods=['GET','POST'])
def select(filename):
    # --- Lectura y limpieza inicial ---
    path = os.path.join(UPLOAD_FOLDER, filename)
    df = (pd.read_excel(path) if filename.lower().endswith(('.xls','.xlsx'))
          else pd.read_csv(path))
    df.columns = df.columns.str.strip()
    df = df[df['JEFE'].apply(normalize)=='ariana micaela alturria']
    # Incluimos ACTIVO para marcar LP
    df = df[['SUPERIOR','NOMBRE','INGRESO','SERVICIO','ACTIVO']].dropna(subset=['SUPERIOR'])
    df['ING_TIME'] = df['INGRESO'].apply(to_time)

    leaders      = sorted(df['SUPERIOR'].unique())
    time_options = [f"{h:02d}:00" for h in range(24)]
    all_services = sorted(df['SERVICIO'].dropna().unique())

    if request.method=='POST':
        # --- 1) Recojo horarios, puestos y lista de skills por líder ---
        start_times   = {L: request.form.get(f'start_{L}') for L in leaders}
        positions     = {L: request.form.get(f'position_{L}') for L in leaders}
        leader_skills = {L: request.form.getlist(f'skill_{L}') for L in leaders}
        # Si algún getlist devuelve [''], normalizamos a lista vacía
        for L, lst in leader_skills.items():
            if lst == [''] or lst == []:
                leader_skills[L] = []

        # --- 2) Construyo ventanas de asignación (t0, t0+ASSIGN_WINDOW) ---
        windows = {}
        windows_str = {}
        for L, st in start_times.items():
            if not st:
                continue
            t0 = datetime.strptime(st,'%H:%M').time()
            end = (datetime.combine(datetime.today(), t0)
                   + timedelta(hours=ASSIGN_WINDOW)).time()
            windows[L] = (t0, end)
            windows_str[L] = (st, end.strftime('%H:%M'))

        # --- 3) Asignación inicial de reps ---
        assignments = {L: [] for L in leaders}
        counts      = {L: 0 for L in leaders}

        # Recorro reps en orden de ingreso
        for _, row in df.sort_values('ING_TIME').iterrows():
            rt  = row['ING_TIME']
            if rt is None:
                continue
            svc = row['SERVICIO']

            # 3.1) Encuentro candidatos válidos:
            #       1) Que caigan en la ventana horaria
            #       2) Si líder tiene lista de skills no vacía, que svc esté en esa lista
            cands = []
            for L,(a,b) in windows.items():
                # Verifico ventana normal o cruzada por medianoche
                in_window = False
                if a <= b:
                    in_window = (a <= rt <= b)
                else:
                    in_window = (rt >= a or rt <= b)
                if not in_window:
                    continue

                sk_list = leader_skills.get(L, [])
                if sk_list:
                    # Si líder L tiene skills definidos, solo acepta si svc coincide con alguno
                    if svc in sk_list:
                        cands.append(L)
                else:
                    # Si no definió ningún skill, acepta cualquiera
                    cands.append(L)

            if not cands:
                continue

            # 3.2) Elijo el candidato con menos reps asignados (balanceo simple)
            chosen = min(cands, key=lambda L: counts[L])
            assignments[chosen].append({
                'rep'     : row['NOMBRE'],
                'service' : svc,
                'ingreso' : rt.strftime('%H:%M'),
                'status'  : row['ACTIVO']
            })
            counts[chosen] += 1

        # --- 4) Guardo en session los datos necesarios para re-eliminaciones/reasignaciones ---
        session['assignments'] = assignments
        session['windows']     = windows_str
        session['positions']   = positions
        session['skills']      = leader_skills
        session['filename']    = filename

        # --- 5) Determino “pusher” (quien está más lejos del ideal según prioridad de puesto) ---
        under = [L for L,c in counts.items() if c < IDEAL_PER_LEADER]
        pushers   = [L for L in under if positions[L]=='Pusher']
        referents = [L for L in under if positions[L]=='Referente de Líder']
        leaders_u = [L for L in under if positions[L]=='Líder']

        if pushers:
            p = min(pushers, key=lambda L: counts[L])
        elif referents:
            p = min(referents, key=lambda L: counts[L])
        elif leaders_u:
            p = min(leaders_u, key=lambda L: counts[L])
        else:
            p = None

        # --- 6) Genero y guardo Excel de salida ---
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as wr:
            # Hoja "Distribuciones"
            rows = []
            for L, infos in assignments.items():
                for info in infos:
                    rows.append({
                        'Líder'   : L,
                        'Ingreso' : info['ingreso'],
                        'Servicio': info['service'],
                        'Rep'     : info['rep'],
                        'Estado'  : info['status']
                    })
            pd.DataFrame(rows).to_excel(wr, index=False, sheet_name='Distribuciones')

            # Hoja "Resumen"
            resumen = []
            for L in leaders:
                cnt = len(assignments[L])
                resumen.append({
                    'Líder'     : L,
                    'Ideal'     : IDEAL_PER_LEADER,
                    'Asignados' : cnt,
                    'Diferencia': cnt - IDEAL_PER_LEADER
                })
            pd.DataFrame(resumen).to_excel(wr, index=False, sheet_name='Resumen')

        output.seek(0)
        with open(os.path.join(OUTPUT_FOLDER, 'Distribuciones.xlsx'), 'wb') as f:
            f.write(output.getvalue())

        # Orden visual: por hora de inicio ascendente
        ordered = sorted(windows.keys(),
                         key=lambda L: datetime.strptime(start_times[L], '%H:%M'))

        return render_template('results.html',
                               assignments=assignments,
                               ordered_leaders=ordered,
                               file_name='Distribuciones.xlsx',
                               positions=positions,
                               skills=leader_skills,
                               pusher=p,
                               ideal=IDEAL_PER_LEADER)

    # GET → mostrar formulario de configuración
    return render_template('select.html',
                           filename=filename,
                           leaders=leaders,
                           time_options=time_options,
                           services=all_services)

@app.route('/eliminate/<leader>', methods=['POST'])
def eliminate(leader):
    # Recupero estado de session
    assignments = session.get('assignments', {})
    windows_str = session.get('windows', {})
    positions   = session.get('positions', {})
    skills      = session.get('skills', {})
    filename    = session.get('filename')

    # Extraigo los reps del líder eliminado
    to_move = assignments.pop(leader, [])

    # Reconstruyo contadores y ventanas sin ese líder
    counts = {L: len(v) for L,v in assignments.items()}
    windows = {}
    for L,(s,e) in windows_str.items():
        if L == leader: 
            continue
        a = datetime.strptime(s, '%H:%M').time()
        b = datetime.strptime(e, '%H:%M').time()
        windows[L] = (a,b)

    # Redistribuyo intentando balancear al ideal
    for info in to_move:
        rt  = datetime.strptime(info['ingreso'], '%H:%M').time()
        svc = info['service']
        valid = []
        for L,(a,b) in windows.items():
            in_window = False
            if a <= b:
                in_window = (a <= rt <= b)
            else:
                in_window = (rt >= a or rt <= b)
            if not in_window:
                continue

            sk_list = skills.get(L, [])
            if sk_list:
                if svc in sk_list:
                    valid.append(L)
            else:
                valid.append(L)

        if not valid:
            continue

        diff = {L: len(assignments[L]) - IDEAL_PER_LEADER for L in valid}
        chosen = min(diff, key=lambda L: diff[L])
        assignments[chosen].append(info)
        counts[chosen] += 1

    session['assignments'] = assignments

    ordered = sorted(windows.keys(),
                     key=lambda L: windows_str[L][0])

    return render_template('results.html',
                           assignments=assignments,
                           ordered_leaders=ordered,
                           file_name='Distribuciones.xlsx',
                           positions=positions,
                           skills=skills,
                           pusher=None,
                           ideal=IDEAL_PER_LEADER)

@app.route('/reassign/<filename>', methods=['POST'])
def reassign(filename):
    raw        = request.form.get('reassignments', '{}')
    new_assign = json.loads(raw)

    path = os.path.join(UPLOAD_FOLDER, session.get('filename'))
    df = (pd.read_excel(path) if path.lower().endswith(('.xls','.xlsx'))
          else pd.read_csv(path))
    df.columns = df.columns.str.strip()
    df = df[df['JEFE'].apply(normalize)=='ariana micaela alturria']
    df = df[['SUPERIOR','NOMBRE','INGRESO','SERVICIO','ACTIVO']]

    assignments = {L: [] for L in new_assign}
    lookup      = df.set_index('NOMBRE').to_dict('index')
    for L,reps in new_assign.items():
        for rep in reps:
            if rep in lookup:
                ent = lookup[rep]
                ing = to_time(ent['INGRESO'])
                assignments[L].append({
                    'rep'     : rep,
                    'service' : ent['SERVICIO'],
                    'ingreso' : ing.strftime('%H:%M') if ing else '',
                    'status'  : ent['ACTIVO']
                })

    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as wr:
        rows = []
        for L, infos in assignments.items():
            for info in infos:
                rows.append({
                    'Líder'   : L,
                    'Ingreso' : info['ingreso'],
                    'Servicio': info['service'],
                    'Rep'     : info['rep'],
                    'Estado'  : info['status']
                })
        pd.DataFrame(rows).to_excel(wr, index=False, sheet_name='Distribuciones')
        resumen = []
        for L, infos in assignments.items():
            cnt = len(infos)
            resumen.append({
                'Líder'     : L,
                'Ideal'     : IDEAL_PER_LEADER,
                'Asignados' : cnt,
                'Diferencia': cnt - IDEAL_PER_LEADER
            })
        pd.DataFrame(resumen).to_excel(wr, index=False, sheet_name='Resumen')
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name='Distribuciones.xlsx',
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.route('/download/<file_name>')
def download(file_name):
    return send_file(
        os.path.join(OUTPUT_FOLDER, file_name),
        as_attachment=True,
        download_name=file_name,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

if __name__ == '__main__':
    app.run(debug=True)
    