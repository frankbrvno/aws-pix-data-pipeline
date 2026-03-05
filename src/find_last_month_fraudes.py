import requests

BASE = "https://olinda.bcb.gov.br/olinda/servico/Pix_DadosAbertos/versao/v1/odata"
HEADERS = {"accept": "application/json;odata.metadata=minimal"}

def url(mes: str) -> str:
    return (
        f"{BASE}/EstatisticasFraudesPix(Database=@Database)"
        f"?%40Database='{mes}'&%24format=json&%24top=1"
    )

def has_data(mes: str) -> bool:
    r = requests.get(url(mes), headers=HEADERS, timeout=60)
    if r.status_code != 200:
        return False
    j = r.json()
    return len(j.get("value", []) or []) > 0

# tenta do mais recente para trás (ajuste o range se quiser)
candidatos = [
    "2026-03","2026-02","2026-01",
    "2025-12","2025-11","2025-10","2025-09","2025-08","2025-07","2025-06",
    "2025-05","2025-04","2025-03","2025-02","2025-01",
]

for m in candidatos:
    print("testando", m, "...")
    if has_data(m):
        print("✅ primeiro mês com dados:", m)
        break
else:
    print("⚠️ nenhum mês com dados no intervalo testado")