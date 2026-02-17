export async function runCode(code, lang = "golang") {
  const endpoint = lang === "python" ? "/api/run/py" : "/api/run/go";

  const res = await fetch(endpoint, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code })
  });

  return res.json();
}