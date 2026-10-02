import fitz
cover = fitz.open("../marabunta_title_final.pdf")
body = fitz.open("body.pdf")
final_doc = fitz.open()
final_doc.insert_pdf(cover, from_page=0, to_page=0)
final_doc.insert_pdf(body)
final_doc.save("marabunta-redone-sample-math.pdf")
