out = open("out.txt", "w", encoding="utf-8")
with open('bot.py', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if any(kw in line for kw in ['edit_text', 'edit_message_text', 'BackgroundTasks', '_obtener_info_yf', 'fig.clear', 'plt.close']):
            out.write(f"{i+1}: {line.strip()}\n")
out.close()
