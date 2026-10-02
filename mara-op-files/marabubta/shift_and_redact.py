import fitz

doc = fitz.open("400_eng_rbattaglia_cv.pdf")

# In this script we do not blindly drop blocks. We use fitz redactions.
# Since we must PRESERVE FORMATTING 100%, and shifting text up and down manually
# is extremely fragile (it breaks page boundaries, bullets, indentations, fonts),
# the ONLY way to achieve 100% preservation is to redact the content blocks in place.
# The user will have white space where the deleted jobs were.

replacements = [
    # (A) Marabunta Date
    {"old": "July 2024 – Present (Nights & Weekends)", "new": "March 2005 – Present (Nights & Weekends)", "page": 0, "font": "LMRoman10-Italic", "size": 9.96},
    # (C) BRQ
    {"old": "November 2019 – July 2020", "new": "November 2018 – January 2021", "page": 1, "font": "LMRoman10-Italic", "size": 9.96},
    # (E) Convexity
    {"old": "July 2010 – August 2013", "new": "December 2009 – October 2014", "page": 2, "font": "LMRoman10-Italic", "size": 9.96},
    # (G) IBM
    {"old": "March 2005 – June 2009", "new": "March 2005 – May 2009", "page": 2, "font": "LMRoman10-Italic", "size": 9.96},
]

ey_old = "Adapting again to a big corporate"
ey_new = "Being back to a big corporate structure is being a great experience, smoother than I expected. Balancing grandfathering with fathering has helped even more that professional coaches."

# We need to redact Eel Ventures completely.
# Find the start of Eel Ventures, and the start of RSI (the next block)
def remove_experience(start_string, end_string, target_page):
    page = doc[target_page]
    start_insts = page.search_for(start_string)
    end_insts = page.search_for(end_string)
    if start_insts and end_insts:
        # Create a redaction rectangle from the top of the start string to the top of the end string
        rect = fitz.Rect(50, start_insts[0].y0 - 2, 550, end_insts[0].y0 - 2)
        page.add_redact_annot(rect, fill=(1,1,1))
        page.apply_redactions()
        print(f"Removed {start_string} block.")

# (C) Remove Eel Ventures
remove_experience("Senior Java Analyst @ Eel Ventures", "Tech Lead & Automation Engineer @ RSI", 1)
# (D) Remove AGS Nasoft
remove_experience("Senior Technical Support & Developer @ AGS Nasoft", "Senior Backend Engineer @ Convexity", 2)

for item in replacements:
    page = doc[item["page"]]
    insts = page.search_for(item["old"])
    for inst in insts:
        page.add_redact_annot(inst, fill=(1,1,1))
        page.apply_redactions()
        # insert_textbox respects the exact bounding box
        # Use a fallback standard font since PyMuPDF can't easily re-insert subsetted embedded fonts
        f_name = "Times-Italic" 
        page.insert_textbox(inst, item["new"], fontname=f_name, fontsize=item["size"], color=(0,0,0))
        print(f"Replaced {item['old']}")

# Fix EY
page0 = doc[0]
ey_insts = page0.search_for(ey_old)
if ey_insts:
    # We redact from the start of the string to the end of the block.
    # The block ends around y=540 according to the previous output.
    rect = fitz.Rect(ey_insts[0].x0, ey_insts[0].y0, 542, 545)
    page0.add_redact_annot(rect, fill=(1,1,1))
    page0.apply_redactions()
    page0.insert_textbox(rect, ey_new, fontname="Times-Roman", fontsize=9.86, color=(0,0,0))
    print("Replaced EY")

doc.save("401_eng_rbattaglia_cv.pdf")
