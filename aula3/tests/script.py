# test_escape.py
import html
request_text = "Linha 1\nLinha 2 com < & >"
escaped = html.escape(request_text)
print(f"Original: '{request_text}'")
print(f"Escaped: '{escaped}'")
print(f"Escaped + replace: '{escaped.replace("\n", "<br>")}'")