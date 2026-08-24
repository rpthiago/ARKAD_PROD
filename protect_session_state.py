import sys
sys.stdout.reconfigure(encoding='utf-8')
import os, re

for fname in sorted(os.listdir('pages')):
    if not fname.endswith('.py'):
        continue
    fpath = os.path.join('pages', fname)
    with open(fpath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Substituir acessos inseguros st.session_state.X != Y por st.session_state.get('X') != Y
    # E st.session_state.X is not None por st.session_state.get('X') is not None
    # E st.session_state.X = Y
    
    orig = content
    # Exemplo: st.session_state.sinais_date != target_date -> st.session_state.get("sinais_date") != target_date
    content = re.sub(r'st\.session_state\.([a-zA-Z0-9_]+)\s*!=\s*', r'st.session_state.get("\1") != ', content)
    content = re.sub(r'st\.session_state\.([a-zA-Z0-9_]+)\s*==\s*', r'st.session_state.get("\1") == ', content)
    content = re.sub(r'st\.session_state\.([a-zA-Z0-9_]+)\s+is\s+not\s+None', r'st.session_state.get("\1") is not None', content)
    content = re.sub(r'st\.session_state\.([a-zA-Z0-9_]+)\s+is\s+None', r'st.session_state.get("\1") is None', content)
    
    if content != orig:
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[+] Protegido session_state em: {fname}")
    else:
        print(f"[-] Sem alteração necessária em: {fname}")
