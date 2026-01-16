# Demo Streamlit - Integracion

Pequena app en Streamlit que simula recoleccion de datos y guarda resultados en SQLite.

## Requisitos
- Python 3.9+
- pip

## Instalacion
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Ejecucion
```bash
streamlit run app.py
```

## Notas de simulacion
- Cada ejecucion genera entre 10 y 30 registros (configurable).
- Los estados posibles son OK, WARN y FAIL.
- El estado del run se resume con prioridad: FAIL > WARN > OK.
- La base local se guarda en `app.db`.
