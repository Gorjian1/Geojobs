import json, os, pandas as pd, httpx, re
from dotenv import load_dotenv

load_dotenv()

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:7b-instruct")

def prompt_for(text: str) -> str:
    with open("prompt.md", "r", encoding="utf-8") as f:
        system_prompt = f.read()
    return f"{system_prompt}\n\nТекст:\n"""\n{text}\n""""

def call_ollama(text: str) -> dict:
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt_for(text),
        "format": "json",
        "options": {"temperature": 0.0}
    }
    url = f"{OLLAMA_HOST}/api/generate"
    out = ""
    with httpx.stream("POST", url, json=payload, timeout=120.0) as resp:
        resp.raise_for_status()
        for line in resp.iter_lines():
            if not line:
                continue
            data = json.loads(line)
            if "response" in data:
                out += data["response"]
            if data.get("done"):
                break
    out = out.strip()
    out = re.sub(r"^```json\s*|```$", "", out, flags=re.IGNORECASE|re.MULTILINE).strip()
    return json.loads(out)

if __name__ == "__main__":
    df = pd.read_csv("../jobs_rows.csv")
    texts = df.iloc[:5, 0].astype(str).tolist()  # assumes the first column holds the raw message
    results = []
    for t in texts:
        try:
            results.append(call_ollama(t))
        except Exception as e:
            results.append({"error": str(e), "source_text": t})
    with open("sample_results.jsonl", "w", encoding="utf-8") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print("Saved sample_results.jsonl")
