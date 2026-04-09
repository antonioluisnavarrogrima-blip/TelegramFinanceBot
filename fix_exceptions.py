def fix_backslash_n():
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()

    # Find the broken literal \n and replace it with real newline
    bad_string = "except Exception as e:\\n            logger.debug(f'Ignorado: {e}')"
    good_string = "except Exception as e:\n            logger.debug(f'Ignorado: {e}')"
    content = content.replace(bad_string, good_string)

    with open('bot.py', 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    fix_backslash_n()
