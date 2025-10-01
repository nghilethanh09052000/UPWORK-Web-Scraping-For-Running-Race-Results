for bib_suffix in range(0, 100):  # 00 to 99
    ...
    formatted_suffix = f"{bib_suffix:02d}"  # Ensures two digits, e.g., 00, 01, ..., 99
    payload = {
        "bib": formatted_suffix

    }
    print(payload)
