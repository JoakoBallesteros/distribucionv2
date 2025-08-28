import os
import sys
import unicodedata
import json
import pandas as pd
from typing import Tuple, List
from io import BytesIO
from datetime import datetime, time, timedelta
from flask import (
    Flask, render_template, request, redirect,
    url_for, send_file, session
)

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev_secret_key')

# Carpetas de entrada/salida
UPLOAD_FOLDER = os.path.abspath(os.path.dirname(__file__))
OUTPUT_FOLDER = os.path.join(UPLOAD_FOLDER, 'outputs')
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Constantes de negocio
IDEAL_PER_LEADER = 21    # ratio objetivo
ASSIGN_WINDOW    = 3     # ventana en horas para asignar reps

# =========================
# Helpers
# =========================
def to_time(val):
    """Convierte distintos formatos de 'INGRESO' a datetime.time."""
    if pd.isna(val):
        return None
    if isinstance(val, time):
        return val
    if isinstance(val, datetime):
        return val.time()
    if isinstance(val, str):
        for fmt in ('%H:%M:%S','%H:%M'):
            try:
                return datetime.strptime(val.strip(), fmt).time()
            except:
                pass
    if isinstance(val, (int, float)):
        try:
            base = datetime(1899,12,30)
            return (base + timedelta(days=float(val))).time()
        except:
            pass
    return None

def normalize(text):
    s = str(text).strip().lower()
    return unicodedata.normalize('NFKD', s)

def rename_to_canon(df: pd.DataFrame) -> pd.DataFrame:
    aliases = {
        'DNI'      : ['dni', 'documento', 'doc', 'id'],
        'USUARIO'  : ['usuario', 'user', 'legajo', 'employee id'],
        'NOMBRE'   : ['nombre', 'name'],
        'SUPERIOR' : ['superior', 'supervisor', 'líder', 'lider'],
        'SERVICIO' : ['servicio', 'skill', 'servicio/skill'],
        'INGRESO'  : ['ingreso', 'hora ingreso', 'ing hora', 'inicio', 'entrada'],
        'ACTIVO'   : ['activo', 'estado'],
        'JEFE'     : ['jefe', 'jefatura', 'manager'],
        'CONTRATO' : ['contrato'],
        'MODALIDAD': ['modalidad'],
    }
    lower_map = {c.strip().lower(): c for c in df.columns}
    mapping = {}
    for canon, opts in aliases.items():
        if canon.lower() in lower_map:
            mapping[lower_map[canon.lower()]] = canon
            continue
        for opt in opts:
            if opt in lower_map:
                mapping[lower_map[opt]] = canon
                break
    if mapping:
        df = df.rename(columns=mapping)
    return df

def filter_by_boss_or_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra por JEFE == BOSS_NAME (env). Si no hay match, retorna todo.
    """
    boss_env = os.getenv('BOSS_NAME', 'ANGEL AGUSTIN ROMERO').strip().lower()
    if 'JEFE' in df.columns:
        mask = df['JEFE'].apply(normalize) == boss_env
        if mask.any():
            return df[mask]
    return df

def is_rrss(val) -> bool:
    """Detecta servicios RRSS/Redes Sociales."""
    if pd.isna(val):
        return False
    s = normalize(val)
    return ('rrss' in s) or ('redes sociales' in s) or ('social media' in s) or ('soporte rrss' in s)

def pick_nomina_sheet(xls: pd.ExcelFile) -> str:
    """Elige la mejor hoja de nómina: prioriza NOMINA AGOSTO, luego otras NOMINA*, evitando RRSS."""
    names = [n.strip() for n in xls.sheet_names]
    preferred = ['NOMINA AGOSTO', 'NOMINA JULIO', 'NOMINA JUNIO', 'NOMINA MAYO']
    for p in preferred:
        for n in names:
            if n.lower() == p.lower():
                return n
    for n in names:
        ln = n.lower()
        if ln.startswith('nomina') and 'rrss' not in ln:
            return n
    return names[0] if names else None

def load_nomina(filename: str, forced_sheet: str = None) -> Tuple[pd.DataFrame, pd.DataFrame, str, List[str]]:
    """
    Carga la nómina (Excel/CSV), normaliza columnas, filtra JEFE y excluye RRSS.
    Devuelve:
      - df: DataFrame normalizado (para lógica)
      - df_original_cols: mismas filas/columnas de la hoja elegida (base para export)
      - selected_sheet: nombre de hoja usada
      - sheet_list: todas las hojas
    """
    path = os.path.join(UPLOAD_FOLDER, filename)
    selected_sheet = None
    sheet_list = []

    if filename.lower().endswith(('.xls', '.xlsx')):
        xls = pd.ExcelFile(path)
        sheet_list = xls.sheet_names
        selected_sheet = forced_sheet if (forced_sheet in sheet_list) else pick_nomina_sheet(xls)
        df_src = pd.read_excel(xls, sheet_name=selected_sheet)
    else:
        df_src = pd.read_csv(path)

    df_original_cols = df_src.copy()

    # Normalización lógica
    df = df_src.copy()
    df.columns = df.columns.map(lambda c: str(c).strip())
    df = rename_to_canon(df)
    df = filter_by_boss_or_all(df)

    # Excluir RRSS si hay columna SERVICIO
    if 'SERVICIO' in df.columns:
        df = df[~df['SERVICIO'].apply(is_rrss)]

    # Columnas mínimas
    required = ['SUPERIOR','NOMBRE','INGRESO','SERVICIO','ACTIVO']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {', '.join(missing)}")

    df = df.dropna(subset=['SUPERIOR']).copy()
    df['ING_TIME'] = df['INGRESO'].apply(to_time)
    return df, df_original_cols, (selected_sheet or ''), sheet_list

# ======= copiar “formato de nómina” (anchos, freeze panes) =======
def clone_layout_from_source(src_path: str, src_sheet: str, dest_ws):
    """Copia ancho de columnas y freeze_panes desde la nómina origen."""
    try:
        wb_src = load_workbook(src_path)
        if src_sheet not in wb_src.sheetnames:
            return
        ws_src = wb_src[src_sheet]
        # Freeze panes
        dest_ws.freeze_panes = ws_src.freeze_panes
        # Column widths
        for col_letter, dim in ws_src.column_dimensions.items():
            if dim.width:
                dest_ws.column_dimensions[col_letter].width = dim.width
    except Exception:
        # Si no se puede leer estilos, seguimos sin romper export
        pass

def write_df_to_ws_preserving_nomina(ws, df_out: pd.DataFrame, number_formats=None):
    """Escribe df en hoja ws preservando encabezados y number formats básicos."""
    # Header
    for j, col in enumerate(df_out.columns, start=1):
        ws.cell(row=1, column=j, value=str(col))
    # Rows
    for i, row in enumerate(df_out.itertuples(index=False, name=None), start=2):
        for j, val in enumerate(row, start=1):
            cell = ws.cell(row=i, column=j, value=val)
            # Formato de hora para 'Ingreso' si llega como time/datetime/str HH:MM
            if number_formats and df_out.columns[j-1] in number_formats:
                fmt = number_formats[df_out.columns[j-1]]
                if isinstance(val, time):
                    cell.value = datetime.combine(datetime(2000,1,1), val)
                    cell.number_format = fmt
                elif isinstance(val, str):
                    try:
                        t = datetime.strptime(val.strip(), "%H:%M").time()
                        cell.value = datetime.combine(datetime(2000,1,1), t)
                        cell.number_format = fmt
                    except:
                        pass

# ======= armado en ORDEN FIJO =======
def build_nomina_export(df_src: pd.DataFrame, df_norm: pd.DataFrame, rep2leader: dict) -> pd.DataFrame:
    """
    Devuelve un DataFrame con EXACTAMENTE este orden y nombres:
    DNI, USUARIO, Nombre, Superior, Servicio, Ingreso, Estado, CONTRATO, MODALIDAD, JEFE, ASIGNADO_A
    """
    # Normalizo headers en copia de la fuente y aplico mapeo canónico
    src = df_src.copy()
    src.columns = src.columns.map(lambda c: str(c).strip())
    src_canon = rename_to_canon(src.copy())

    # Quedarme solo con filas asignadas (por NOMBRE)
    if 'NOMBRE' in src_canon.columns:
        mask = src_canon['NOMBRE'].astype(str).isin(rep2leader.keys())
        src_canon = src_canon[mask].copy()
    else:
        # Fallback a df_norm si la hoja original no trae NOMBRE
        src_canon = df_norm.copy()

    # Mapa canónico -> etiqueta final pedida
    canon_to_out = {
        'DNI'      : 'DNI',
        'USUARIO'  : 'USUARIO',
        'NOMBRE'   : 'Nombre',
        'SUPERIOR' : 'Superior',
        'SERVICIO' : 'Servicio',
        'INGRESO'  : 'Ingreso',
        'ACTIVO'   : 'Estado',
        'CONTRATO' : 'CONTRATO',
        'MODALIDAD': 'MODALIDAD',
        'JEFE'     : 'JEFE',
    }
    desired_order = ['DNI','USUARIO','Nombre','Superior','Servicio','Ingreso','Estado','CONTRATO','MODALIDAD','JEFE']

    # Armar columnas en el orden requerido (columna faltante -> vacía)
    cols_data = {}
    for canon, out_name in canon_to_out.items():
        if canon in src_canon.columns:
            cols_data[out_name] = src_canon[canon].values
        else:
            cols_data[out_name] = [''] * len(src_canon)

    df_out = pd.DataFrame(cols_data, index=src_canon.index)[desired_order]

    # Columna final ASIGNADO_A mapeando por NOMBRE
    if 'NOMBRE' in src_canon.columns:
        nombres = src_canon['NOMBRE'].astype(str)
        df_out['ASIGNADO_A'] = nombres.map(rep2leader).fillna('')
    else:
        df_out['ASIGNADO_A'] = ''

    return df_out

# =========================
# Rutas
# =========================
@app.route('/', methods=['GET','POST'])
def upload():
    if request.method == 'POST':
        f = request.files.get('file')
        if not f:
            return render_template('upload.html', error='Debes subir un archivo')
        fn = f.filename
        f.save(os.path.join(UPLOAD_FOLDER, fn))
        return redirect(url_for('select', filename=fn))
    return render_template('upload.html')

@app.route('/select/<filename>', methods=['GET','POST'])
def select(filename):
    forced_sheet = request.args.get('sheet') if request.method == 'GET' else request.form.get('sheet')

    try:
        df, df_src, selected_sheet, all_sheets = load_nomina(filename, forced_sheet=forced_sheet)
    except Exception as e:
        return render_template('upload.html', error=f"Error leyendo la nómina: {e}")

    leaders      = sorted(df['SUPERIOR'].dropna().astype(str).unique())
    time_options = [f"{h:02d}:00" for h in range(24)]
    all_services = sorted(df['SERVICIO'].dropna().astype(str).unique())

    if request.method == 'POST':
        # --- 1) Recojo horarios, puestos y lista de skills por líder ---
        start_times   = {L: request.form.get(f'start_{L}') for L in leaders}
        positions     = {L: request.form.get(f'position_{L}') for L in leaders}
        leader_skills = {L: request.form.getlist(f'skill_{L}') for L in leaders}
        for L, lst in list(leader_skills.items()):
            if not lst or lst == ['']:
                leader_skills[L] = []

        # --- 2) Ventanas de asignación ---
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

        for _, row in df.sort_values('ING_TIME').iterrows():
            rt  = row['ING_TIME']
            if rt is None:
                continue
            svc = str(row['SERVICIO'])

            # 3.1) Candidatos válidos por ventana y skill
            cands = []
            for L, ab in windows.items():
                if not ab:
                    continue
                a, b = ab
                in_window = (a <= rt <= b) if (a <= b) else (rt >= a or rt <= b)
                if not in_window:
                    continue
                sk_list = leader_skills.get(L, [])
                if sk_list:
                    if svc in sk_list:
                        cands.append(L)
                else:
                    cands.append(L)

            if not cands:
                continue

            # 3.2) Elegir el menos cargado
            chosen = min(cands, key=lambda L: counts[L])
            assignments[chosen].append({
                'rep'     : row['NOMBRE'],
                'service' : svc,
                'ingreso' : rt.strftime('%H:%M'),
                'status'  : row['ACTIVO']
            })
            counts[chosen] += 1

        # --- 4) Persisto en session ---
        session['assignments']    = assignments
        session['windows']        = windows_str
        session['positions']      = positions
        session['skills']         = leader_skills
        session['filename']       = filename
        session['selected_sheet'] = selected_sheet
        session['src_cols']       = list(df_src.columns)  # orden original

        # --- 5) ¿Quién empuja? ---
        under = [L for L,c in counts.items() if c < IDEAL_PER_LEADER]
        pushers   = [L for L in under if positions.get(L) == 'Pusher']
        referents = [L for L in under if positions.get(L) == 'Referente de Líder']
        leaders_u = [L for L in under if positions.get(L) == 'Líder']

        if pushers:
            p = min(pushers, key=lambda L: counts[L])
        elif referents:
            p = min(referents, key=lambda L: counts[L])
        elif leaders_u:
            p = min(leaders_u, key=lambda L: counts[L])
        else:
            p = None

        # --- 6) DataFrame final con ORDEN FIJO + ASIGNADO_A ---
        rep2leader = {}
        for L, infos in assignments.items():
            for info in infos:
                rep2leader[info['rep']] = L

        df_out = build_nomina_export(df_src, df, rep2leader)

        # === Crear workbook clonando formato básico de la nómina ===
        output = BytesIO()
        wb = Workbook()
        # borrar hoja default
        wb.remove(wb.active)
        ws = wb.create_sheet(title=(selected_sheet or 'NOMINA'))

        # Copiar layout (anchos y freeze panes) desde archivo fuente
        src_path = os.path.join(UPLOAD_FOLDER, filename)
        clone_layout_from_source(src_path, selected_sheet, ws)

        # Formato de hora (coincide con nombre de columna de salida: 'Ingreso')
        number_formats = {'Ingreso': 'hh:mm'}

        # Escribir data preservando formatos
        write_df_to_ws_preserving_nomina(ws, df_out, number_formats=number_formats)

        # Hoja Resumen
        ws2 = wb.create_sheet(title='Resumen')
        ws2.append(['Líder', 'Ideal', 'Asignados', 'Diferencia'])
        for L in leaders:
            cnt = len(assignments[L])
            ws2.append([L, IDEAL_PER_LEADER, cnt, cnt - IDEAL_PER_LEADER])

        wb.save(output)
        output.seek(0)

        # Guardar también en disco para /download
        with open(os.path.join(OUTPUT_FOLDER, 'Nomina_asignada.xlsx'), 'wb') as f:
            f.write(output.getvalue())

        ordered = sorted(windows.keys(),
                         key=lambda L: datetime.strptime(start_times[L], '%H:%M'))

        return render_template(
            'results.html',
            assignments=assignments,
            ordered_leaders=ordered,
            file_name='Nomina_asignada.xlsx',
            positions=positions,
            skills=leader_skills,
            pusher=p,
            ideal=IDEAL_PER_LEADER,
            filename=filename,  # necesario para url_for('reassign', filename=filename)
        )

    # GET → mostrar formulario de configuración
    return render_template('select.html',
                           filename=filename,
                           leaders=leaders,
                           time_options=time_options,
                           services=all_services,
                           sheets=all_sheets,
                           selected_sheet=selected_sheet)

# Reasignar (tolerante con y sin filename para evitar 404)
@app.route('/reassign/', defaults={'filename': None}, methods=['POST'])
@app.route('/reassign/<path:filename>', methods=['POST'])
def reassign(filename):
    raw        = request.form.get('reassignments', '{}')
    new_assign = json.loads(raw)

    if not filename:
        filename = session.get('filename')

    forced_sheet = session.get('selected_sheet')
    try:
        df, df_src, selected_sheet, _ = load_nomina(session.get('filename'), forced_sheet=forced_sheet)
    except Exception:
        output = BytesIO()
        wb = Workbook()
        ws = wb.active
        ws.title = selected_sheet or 'NOMINA'
        wb.save(output)
        output.seek(0)
        return send_file(
            output,
            as_attachment=True,
            download_name='Nomina_asignada.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    # Mapa rep -> líder según la reasignación
    rep2leader = {}
    lookup = df.set_index('NOMBRE').to_dict('index')
    for L, reps in new_assign.items():
        for rep in reps:
            if rep in lookup:
                rep2leader[rep] = L

    # DataFrame final con ORDEN FIJO + ASIGNADO_A
    df_out = build_nomina_export(df_src, df, rep2leader)

    # Crear workbook con formato de la nómina
    output = BytesIO()
    wb = Workbook()
    wb.remove(wb.active)
    ws = wb.create_sheet(title=(selected_sheet or 'NOMINA'))
    src_path = os.path.join(UPLOAD_FOLDER, filename)
    clone_layout_from_source(src_path, selected_sheet, ws)
    write_df_to_ws_preserving_nomina(ws, df_out, number_formats={'Ingreso': 'hh:mm'})

    # Resumen
    from collections import Counter
    c = Counter(rep2leader.values())
    ws2 = wb.create_sheet(title='Resumen')
    ws2.append(['Líder', 'Ideal', 'Asignados', 'Diferencia'])
    for L, cnt in sorted(c.items()):
        ws2.append([L, IDEAL_PER_LEADER, cnt, cnt - IDEAL_PER_LEADER])

    wb.save(output)
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name='Nomina_asignada.xlsx',
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
    print(f"Python: {sys.version}")
    print(">>> Iniciando Flask en http://127.0.0.1:5000 (CTRL+C para salir)")
    if os.name == "nt":
        try:
            import asyncio
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass
    app.run(host='127.0.0.1', port=5000, debug=True)
