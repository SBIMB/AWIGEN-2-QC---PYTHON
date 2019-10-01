from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

canvas = canvas.Canvas("form.pdf", pagesize=letter)
canvas.setLineWidth(.3)
canvas.setFont('Helvetica', 12)

canvas.drawString(30, 750, 'OFFICIAL REPORT')
canvas.drawString(30, 735, 'OF SOWETO DATA')
canvas.drawString(500, 750, 'DATE')
canvas.line(480, 747, 580, 747)

canvas.line(378, 723, 580, 723)

canvas.drawString(30, 703, 'SENT BY:')
canvas.line(120, 700, 580, 700)
canvas.drawString(120, 703, "SBIMB")

canvas.save()
