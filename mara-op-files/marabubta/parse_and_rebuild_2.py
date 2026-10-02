import fitz

doc = fitz.open("400_eng_rbattaglia_cv.pdf")

for page_num in range(len(doc)):
    page = doc[page_num]
    text_blocks = page.get_text("blocks")
    for b in text_blocks:
        x0, y0, x1, y1, text, block_no, block_type = b
        if block_type == 0:  # text block
            if "Adapting again to a big corporate" in text:
                print(f"Page {page_num}: Bbox: {(x0, y0, x1, y1)}, Text:\n{text[:50]}...")
            elif "November 2019" in text:
                print(f"Page {page_num}: Bbox: {(x0, y0, x1, y1)}, Text:\n{text[:50]}...")
            elif "Eel Ventures" in text:
                print(f"Page {page_num}: Bbox: {(x0, y0, x1, y1)}, Text:\n{text[:50]}...")
            elif "AGS Nasoft" in text:
                print(f"Page {page_num}: Bbox: {(x0, y0, x1, y1)}, Text:\n{text[:50]}...")
            elif "Convexity" in text:
                print(f"Page {page_num}: Bbox: {(x0, y0, x1, y1)}, Text:\n{text[:50]}...")
            elif "IBM" in text:
                print(f"Page {page_num}: Bbox: {(x0, y0, x1, y1)}, Text:\n{text[:50]}...")
            elif "Unisys" in text:
                print(f"Page {page_num}: Bbox: {(x0, y0, x1, y1)}, Text:\n{text[:50]}...")

