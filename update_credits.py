import sqlite3
import traceback

def update_admin():
    try:
        conn = sqlite3.connect('botfinanzas.db', timeout=10)
        c = conn.cursor()
        c.execute('UPDATE usuarios SET creditos = 100000')
        conn.commit()
        
        c.execute('SELECT telegram_id, creditos FROM usuarios')
        rows = c.fetchall()
        with open('output.txt', 'w') as f:
            f.write("Usuarios actuales:\n")
            for r in rows:
                f.write(f"{r}\n")
        conn.close()
    except Exception as e:
        with open('output.txt', 'w') as f:
            f.write(f"ERROR: {e}\n{traceback.format_exc()}")

if __name__ == "__main__":
    update_admin()
