#!/usr/bin/env python3
"""VelOzity invoice PDF generator — called from server.js"""
import sys, json, argparse
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                 TableStyle, HRFlowable, KeepTogether)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER

# ── Brand colors ──
BRAND     = colors.HexColor('#990033')
DARK      = colors.HexColor('#1C1C1E')
MID       = colors.HexColor('#6E6E73')
LIGHT     = colors.HexColor('#AEAEB2')
BG_LIGHT  = colors.HexColor('#F5F5F7')
WHITE     = colors.white
BLACK     = colors.black
LINE      = colors.HexColor('#E5E5EA')

W, H = A4  # 595.28 x 841.89 pts

def fmt_usd(v):
    try:
        f = float(v or 0)
        return f'USD {f:,.2f}'
    except:
        return 'USD 0.00'

def fmt_num(v):
    try:
        f = float(v or 0)
        return f'{f:,.2f}' if f != int(f) else f'{int(f):,}'
    except:
        return '0'

def build_pdf(inv: dict, out_path: str):
    doc = SimpleDocTemplate(
        out_path, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm,
        topMargin=18*mm, bottomMargin=18*mm,
    )

    styles = getSampleStyleSheet()
    def sty(name, **kw):
        base = styles.get(name, styles['Normal'])
        return ParagraphStyle('_', parent=base, **kw)

    h1    = sty('Normal', fontSize=22, fontName='Helvetica-Bold', textColor=DARK, spaceAfter=2)
    h2    = sty('Normal', fontSize=13, fontName='Helvetica-Bold', textColor=DARK, spaceAfter=2)
    h3    = sty('Normal', fontSize=10, fontName='Helvetica-Bold', textColor=DARK)
    body  = sty('Normal', fontSize=9,  fontName='Helvetica',      textColor=DARK)
    small = sty('Normal', fontSize=8,  fontName='Helvetica',      textColor=MID)
    label = sty('Normal', fontSize=8,  fontName='Helvetica-Bold', textColor=BRAND)
    right = sty('Normal', fontSize=9,  fontName='Helvetica',      textColor=DARK, alignment=TA_RIGHT)
    right_b = sty('Normal', fontSize=9, fontName='Helvetica-Bold', textColor=DARK, alignment=TA_RIGHT)
    total_sty = sty('Normal', fontSize=11, fontName='Helvetica-Bold', textColor=WHITE)
    total_r   = sty('Normal', fontSize=11, fontName='Helvetica-Bold', textColor=WHITE, alignment=TA_RIGHT)

    story = []

    # ── Header ──
    inv_type = inv.get('type','VAS')
    ref = inv.get('ref_number','—')
    inv_date = inv.get('invoice_date','') or ''
    due_date = inv.get('due_date','') or ''
    week_start = inv.get('week_start','')

    header_data = [[
        Paragraph('<b>VelOzity</b>', sty('Normal', fontSize=18, fontName='Helvetica-Bold', textColor=BRAND)),
        Paragraph('TAX INVOICE', sty('Normal', fontSize=16, fontName='Helvetica-Bold', textColor=DARK, alignment=TA_RIGHT))
    ]]
    header_tbl = Table(header_data, colWidths=[W*0.5 - 20*mm, W*0.5 - 20*mm])
    header_tbl.setStyle(TableStyle([('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BOTTOMPADDING',(0,0),(-1,-1),4)]))
    story.append(header_tbl)
    story.append(HRFlowable(width='100%', thickness=1.5, color=BRAND, spaceAfter=8))

    # ── Company + Invoice meta ──
    co_lines = [
        Paragraph('Ogeo Pty Ltd.', h3),
        Paragraph('ABN: 96 670 485 499', small),
        Paragraph('9 Aquamarine Street, Quakers Hill NSW 2763', small),
        Paragraph('shuch@velozity.au  |  +61-449-701-751', small),
    ]
    meta_lines = [
        Paragraph(f'<b>Invoice No:</b> {ref}', body),
        Paragraph(f'<b>Invoice Date:</b> {inv_date}', body),
        Paragraph(f'<b>Due Date:</b> {due_date}', body),
        Paragraph(f'<b>Week:</b> {week_start}', body),
        Paragraph(f'<b>Incoterm:</b> DAP', body),
        Paragraph(f'<b>Payment Terms:</b> {"30 Days" if inv_type=="VAS" else "7 Days"} from Invoice', body),
    ]
    meta_tbl = Table([[co_lines, meta_lines]], colWidths=[W*0.5 - 20*mm, W*0.5 - 20*mm])
    meta_tbl.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING', (0,0), (-1,-1), 0),
        ('RIGHTPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 8))

    # ── Bill To ──
    story.append(HRFlowable(width='100%', thickness=0.5, color=LINE, spaceBefore=4, spaceAfter=6))
    story.append(Paragraph('BILL TO', label))
    story.append(Paragraph('The Iconic [ABN 50 152 631 082]', h3))
    story.append(Paragraph('Level 18, Tower Two, International Towers, 200 Barangaroo Avenue, Barangaroo NSW 2000', body))
    story.append(Spacer(1, 8))

    # ── Description of Services ──
    story.append(HRFlowable(width='100%', thickness=0.5, color=LINE, spaceBefore=2, spaceAfter=6))
    story.append(Paragraph('DESCRIPTION OF SERVICES', label))
    if inv_type == 'VAS':
        story.append(Paragraph(
            'Services provided to The Iconic by VelOzity: VAS base processing, outbound activities, '
            'additional labelling, and carton replacement labour as detailed below.',
            body))
    elif inv_type == 'SEA':
        story.append(Paragraph(
            'Services provided to The Iconic by VelOzity: transportation from warehouse to port, '
            'origin customs clearing and customs declaration, sea freight, destination customs '
            'declaration and clearing, and transportation from port to FC Yennora.',
            body))
    elif inv_type == 'AIR':
        story.append(Paragraph(
            'Services provided to The Iconic by VelOzity: transportation from warehouse to airport, '
            'origin customs clearing, customs declaration, air freight, destination customs '
            'declaration and clearing, and transportation from airport to FC Yennora.',
            body))
    story.append(Spacer(1, 10))

    lines = inv.get('lines', [])

    # ── VAS Invoice Lines ──
    if inv_type == 'VAS':
        story.append(Paragraph('DETAILS OF CHARGES (USD)', label))
        story.append(Spacer(1, 4))
        tbl_data = [
            [Paragraph('<b>Service</b>', h3),
             Paragraph('<b>Unit</b>', h3),
             Paragraph('<b>Rate</b>', h3),
             Paragraph('<b>Quantity</b>', sty('Normal', fontSize=9, fontName='Helvetica-Bold', alignment=TA_RIGHT)),
             Paragraph('<b>USD Total</b>', sty('Normal', fontSize=9, fontName='Helvetica-Bold', alignment=TA_RIGHT))],
        ]
        main_lines = [l for l in lines if not l.get('gst_free') and not l.get('is_misc')]
        misc_lines = [l for l in lines if l.get('is_misc') and l.get('description')]
        for l in main_lines:
            tbl_data.append([
                Paragraph(l.get('description',''), body),
                Paragraph(l.get('unit_label',''), small),
                Paragraph(fmt_num(l.get('rate',0)), body),
                Paragraph(fmt_num(l.get('quantity',0)), right),
                Paragraph(fmt_usd(l.get('total',0)), right),
            ])
        for l in misc_lines:
            tbl_data.append([
                Paragraph(l.get('description',''), body),
                Paragraph(l.get('unit_label',''), small),
                Paragraph(fmt_num(l.get('rate',0)) if l.get('rate') else '', body),
                Paragraph(fmt_num(l.get('quantity',0)) if l.get('quantity') else '', right),
                Paragraph(fmt_usd(l.get('total',0)) if l.get('total') else '', right),
            ])
        tbl = Table(tbl_data, colWidths=[80*mm, 35*mm, 20*mm, 25*mm, 30*mm])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), BG_LIGHT),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [WHITE, colors.HexColor('#FAFAFA')]),
            ('GRID', (0,0), (-1,-1), 0.3, LINE),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,-1), 9),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('LEFTPADDING', (0,0), (-1,-1), 6),
            ('RIGHTPADDING', (0,0), (-1,-1), 6),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 10))

    # ── SEA Invoice Lines ──
    elif inv_type == 'SEA':
        freight_lines = [l for l in lines if not l.get('gst_free') and not l.get('is_misc')]
        customs_lines = [l for l in lines if l.get('gst_free') and not l.get('is_misc')]
        misc_lines    = [l for l in lines if l.get('is_misc') and l.get('description')]

        if freight_lines:
            story.append(Paragraph('SEA FREIGHT CHARGES (USD)', label))
            story.append(Spacer(1, 4))
            hdr = [Paragraph(t, h3) for t in ['Container Number','Container Type','Rate per Container (USD)','Cost per Container (USD)']]
            rows = [hdr]
            for l in freight_lines:
                rows.append([
                    Paragraph(l.get('container_id') or l.get('description',''), body),
                    Paragraph(l.get('unit_label',''), body),
                    Paragraph(fmt_usd(l.get('rate',0)), body),
                    Paragraph(fmt_usd(l.get('total',0)), right),
                ])
            for l in misc_lines:
                rows.append([Paragraph(l.get('description',''), body), Paragraph('',''), Paragraph('',''), Paragraph(fmt_usd(l.get('total',0)) if l.get('total') else '', right)])
            tbl = Table(rows, colWidths=[50*mm, 35*mm, 55*mm, 50*mm])
            tbl.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), BG_LIGHT),
                ('ROWBACKGROUNDS', (0,1), (-1,-1), [WHITE, colors.HexColor('#FAFAFA')]),
                ('GRID', (0,0), (-1,-1), 0.3, LINE),
                ('FONTSIZE', (0,0), (-1,-1), 9),
                ('TOPPADDING', (0,0), (-1,-1), 5), ('BOTTOMPADDING', (0,0), (-1,-1), 5),
                ('LEFTPADDING', (0,0), (-1,-1), 6), ('RIGHTPADDING', (0,0), (-1,-1), 6),
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ]))
            story.append(tbl)
            story.append(Spacer(1, 8))

        if customs_lines:
            story.append(Paragraph('CUSTOMS CLEARANCE (GST-FREE)', label))
            story.append(Spacer(1, 4))
            hdr = [Paragraph(t, h3) for t in ['Container Number','Fee Description','Flat Fee (USD)','Cost (USD)']]
            rows = [hdr]
            for l in customs_lines:
                rows.append([
                    Paragraph(l.get('container_id') or l.get('description',''), body),
                    Paragraph('Flat Fee per Container for Customs Clearance', body),
                    Paragraph(fmt_usd(l.get('rate',0)), body),
                    Paragraph(fmt_usd(l.get('total',0)), right),
                ])
            tbl = Table(rows, colWidths=[40*mm, 70*mm, 35*mm, 45*mm])
            tbl.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), BG_LIGHT),
                ('GRID', (0,0), (-1,-1), 0.3, LINE),
                ('FONTSIZE', (0,0), (-1,-1), 9),
                ('TOPPADDING', (0,0), (-1,-1), 5), ('BOTTOMPADDING', (0,0), (-1,-1), 5),
                ('LEFTPADDING', (0,0), (-1,-1), 6), ('RIGHTPADDING', (0,0), (-1,-1), 6),
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ]))
            story.append(tbl)
            story.append(Spacer(1, 8))

    # ── AIR Invoice Lines ──
    elif inv_type == 'AIR':
        freight_lines = [l for l in lines if not l.get('gst_free') and not l.get('is_misc')]
        customs_lines = [l for l in lines if l.get('gst_free') and not l.get('is_misc')]
        misc_lines    = [l for l in lines if l.get('is_misc') and l.get('description')]

        story.append(Paragraph('AIR FREIGHT CHARGES (USD)', label))
        story.append(Spacer(1, 4))
        hdr = [Paragraph(t, h3) for t in ['Description','Supplier / Zendesk','Rate (USD)','Unit','Total (USD)']]
        rows = [hdr]
        for l in freight_lines:
            zd = l.get('zendesk','')
            sup = l.get('supplier','') or l.get('description','')
            rows.append([
                Paragraph(l.get('description','Air Freight'), body),
                Paragraph(f'{sup}\nZendesk #{zd}' if zd else sup, small),
                Paragraph(fmt_usd(l.get('rate',0)), body),
                Paragraph(l.get('unit_label','Per KG'), body),
                Paragraph(fmt_usd(l.get('total',0)), right),
            ])
        for l in misc_lines:
            rows.append([Paragraph(l.get('description',''), body), Paragraph('',''), Paragraph('',''), Paragraph('',''), Paragraph(fmt_usd(l.get('total',0)) if l.get('total') else '', right)])
        tbl = Table(rows, colWidths=[45*mm, 50*mm, 25*mm, 20*mm, 50*mm])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), BG_LIGHT),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [WHITE, colors.HexColor('#FAFAFA')]),
            ('GRID', (0,0), (-1,-1), 0.3, LINE),
            ('FONTSIZE', (0,0), (-1,-1), 9),
            ('TOPPADDING', (0,0), (-1,-1), 5), ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('LEFTPADDING', (0,0), (-1,-1), 6), ('RIGHTPADDING', (0,0), (-1,-1), 6),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 8))

        if customs_lines:
            story.append(Paragraph('CUSTOMS PROCESSING (GST-FREE)', label))
            story.append(Spacer(1, 4))
            hdr = [Paragraph(t, h3) for t in ['Description','Rate (USD)','Quantity','Total (USD)']]
            rows = [hdr]
            for l in customs_lines:
                rows.append([
                    Paragraph(l.get('description','Customs Processing'), body),
                    Paragraph(fmt_usd(l.get('rate',0)), body),
                    Paragraph(fmt_num(l.get('quantity',1)), body),
                    Paragraph(fmt_usd(l.get('total',0)), right),
                ])
            tbl = Table(rows, colWidths=[70*mm, 35*mm, 30*mm, 55*mm])
            tbl.setStyle(TableStyle([
                ('BACKGROUND', (0,0), (-1,0), BG_LIGHT),
                ('GRID', (0,0), (-1,-1), 0.3, LINE),
                ('FONTSIZE', (0,0), (-1,-1), 9),
                ('TOPPADDING', (0,0), (-1,-1), 5), ('BOTTOMPADDING', (0,0), (-1,-1), 5),
                ('LEFTPADDING', (0,0), (-1,-1), 6), ('RIGHTPADDING', (0,0), (-1,-1), 6),
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ]))
            story.append(tbl)
            story.append(Spacer(1, 8))

    # ── Totals block ──
    story.append(HRFlowable(width='100%', thickness=0.5, color=LINE, spaceBefore=4, spaceAfter=6))
    subtotal = float(inv.get('subtotal') or 0)
    gst      = float(inv.get('gst') or 0)
    customs  = float(inv.get('customs') or 0)
    misc     = float(inv.get('misc_total') or 0)
    total    = float(inv.get('total') or 0)

    tot_rows = []
    if subtotal: tot_rows.append(['Subtotal (excl. GST)', fmt_usd(subtotal)])
    if gst:      tot_rows.append([f'GST at 10%', fmt_usd(gst)])
    if customs:  tot_rows.append(['Customs Clearance (GST-free)', fmt_usd(customs)])
    if misc:     tot_rows.append(['Additional Charges', fmt_usd(misc)])

    if tot_rows:
        tbl_data = [[Paragraph(r[0], body), Paragraph(r[1], right)] for r in tot_rows]
        tbl_data.append([
            Paragraph('<b>Total Payable</b>', sty('Normal', fontSize=10, fontName='Helvetica-Bold', textColor=WHITE)),
            Paragraph(f'<b>{fmt_usd(total)}</b>', sty('Normal', fontSize=10, fontName='Helvetica-Bold', textColor=WHITE, alignment=TA_RIGHT))
        ])
        tot_tbl = Table(tbl_data, colWidths=[W - 40*mm - 60*mm, 60*mm], hAlign='RIGHT')
        tot_tbl.setStyle(TableStyle([
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('RIGHTPADDING', (0,0), (-1,-1), 8),
            ('LINEABOVE', (0,-1), (-1,-1), 0.5, LINE),
            ('BACKGROUND', (0,-1), (-1,-1), BRAND),
            ('ROWBACKGROUNDS', (0,0), (-1,-2), [colors.HexColor('#FAFAFA'), WHITE]),
        ]))
        story.append(tot_tbl)

    story.append(Spacer(1, 16))

    # ── Notes ──
    notes = inv.get('notes','')
    if notes:
        story.append(HRFlowable(width='100%', thickness=0.5, color=LINE, spaceAfter=6))
        story.append(Paragraph('NOTES', label))
        story.append(Paragraph(notes, body))
        story.append(Spacer(1, 10))

    # ── Remittance ──
    story.append(HRFlowable(width='100%', thickness=1, color=BRAND, spaceBefore=8, spaceAfter=8))
    story.append(Paragraph('REMITTANCE INFORMATION', label))
    story.append(Spacer(1, 4))
    remit = [
        ['Account Beneficiary Name', 'OGEO PTY LTD'],
        ['Bank Name', 'COMMONWEALTH BANK'],
        ['Bank Address', '2 Sentry Dr, Stanhope Gardens NSW 2768, Australia'],
        ['Bank Account Number', '10199366'],
        ['SWIFT Code', 'CTBAAU2S'],
        ['BSB / IBAN', '062-704'],
    ]
    rem_tbl = Table([[Paragraph(r[0], sty('Normal', fontSize=8, fontName='Helvetica-Bold', textColor=MID)),
                      Paragraph(r[1], body)] for r in remit],
                    colWidths=[55*mm, W - 40*mm - 55*mm])
    rem_tbl.setStyle(TableStyle([
        ('TOPPADDING', (0,0), (-1,-1), 3), ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ('LEFTPADDING', (0,0), (-1,-1), 0), ('RIGHTPADDING', (0,0), (-1,-1), 0),
        ('LINEBELOW', (0,0), (-1,-2), 0.3, LINE),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(rem_tbl)

    doc.build(story)
    print(f'PDF written to {out_path}', file=sys.stderr)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--invoice', required=True, help='JSON string of invoice object')
    parser.add_argument('--out', required=True, help='Output PDF path')
    args = parser.parse_args()
    inv = json.loads(args.invoice)
    build_pdf(inv, args.out)
