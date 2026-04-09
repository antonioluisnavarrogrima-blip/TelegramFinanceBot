def fix_all_exceptions():
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # We want to replace the broken multiline ones with a single line:
    content = content.replace("except Exception as e:\n            logger.debug(f'Ignorado: {e}')", "except Exception as e: logger.debug(f'Ignorado: {e}')")

    with open('bot.py', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    fix_all_exceptions()
