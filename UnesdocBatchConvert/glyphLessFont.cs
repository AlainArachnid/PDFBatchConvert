using System;
using iText.Kernel.Pdf;
using iText.Kernel.Pdf.Canvas;
using iText.Kernel.Pdf.Canvas.Parser;
using iText.Kernel.Pdf.Canvas.Parser.Listener;
using iText.Kernel.Pdf.Canvas.Parser.Data;
using iText.Kernel.Utils;
using iText.Kernel.Pdf.Xobject;
using iText.Kernel.Pdf.Tagging;
using iText.Kernel.Font;
using iText.IO.Font;
using iText.IO.Font.Cmap;

class GlyphLessFont
{
    const int KCHARWIDTH = 2;
    public const String FONTNAME_GLYPHLESS = "GlyphLessFont";
    const String TOUNICODE =
    "/CIDInit /ProcSet findresource begin\n" +
        "12 dict begin\n" +
        "begincmap\n" +
        "/CIDSystemInfo\n" +
        "<<\n" +
        "  /Registry (Adobe)\n" +
        "  /Ordering (UCS)\n" +
        "  /Supplement 0\n" +
        ">> def\n" +
        "/CMapName /Adobe-Identify-UCS def\n" +
        "/CMapType 2 def\n" +
        "1 begincodespacerange\n" +
        "<0000> <FFFF>\n" +
        "endcodespacerange\n" +
        "1 beginbfrange\n" +
        "<0000> <FFFF> <0000>\n" +
        "endbfrange\n" +
        "endcmap\n" +
        "CMapName currentdict /CMap defineresource pop\n" +
        "end\n" +
        "end\n";
    const int KCIDTOGIDMAPSIZE = 2 * (1 << 16);

    static private byte[] _toUnicode = null;
    static private byte[] _cidtogidmap = null;
    static readonly PdfName PDFNAME_GLYPHLESS = new PdfName(FONTNAME_GLYPHLESS);

    public static PdfFont AddFromFileToPdf(PdfDocument document, String fontFilePath)
    {
        PdfType0Font font0 = (PdfType0Font)PdfFontFactory.CreateFont(fontFilePath, iText.IO.Font.PdfEncodings.IDENTITY_H, document);
        Console.WriteLine("font type : " + font0.GetType().ToString());
        var streamCMap = font0.GetToUnicode();
        if (streamCMap == null)
            Console.WriteLine("no cMap stream");
        else
            Console.WriteLine("cMap stream length : " + streamCMap.GetLength());
        var cmap = font0.GetCmap();
        Console.WriteLine(cmap.GetCmapName());
        return font0;
    }

    public static PdfFont AddToPdfDocument(PdfDocument document, String fontFilePath)
    {

        // FILE
        var filebytes = System.IO.File.ReadAllBytes(fontFilePath);
        var streamFontFile = new PdfStream(filebytes);

        // FONT DESCRIPTOR
        var dicFontDescriptor = new PdfDictionary();
        dicFontDescriptor.Put(PdfName.Ascent, new PdfNumber(1000));
        dicFontDescriptor.Put(PdfName.CapHeight, new PdfNumber(1000));
        dicFontDescriptor.Put(PdfName.Descent, new PdfNumber(0));  // Spec says must be negative
        dicFontDescriptor.Put(PdfName.Flags, new PdfNumber(65569));  // 5 : FixedPitch + Symbolic, 65569 in scanned files
        dicFontDescriptor.Put(PdfName.FontBBox, new PdfArray(new int[] { 0, 0, (1000 / KCHARWIDTH), 1000 }));
        //dicFontDescriptor.Put(new PdfName("FontFile2"), fileEntry.GetK().GetIndirectReference());
        dicFontDescriptor.Put(PdfName.FontName, PDFNAME_GLYPHLESS);
        dicFontDescriptor.Put(new PdfName("AvgWidth"), new PdfNumber(1000 / KCHARWIDTH));
        dicFontDescriptor.Put(new PdfName("MaxWidth"), new PdfNumber(1000 / KCHARWIDTH));
        dicFontDescriptor.Put(new PdfName("MissingWidth"), new PdfNumber(1000 / KCHARWIDTH));
        dicFontDescriptor.Put(PdfName.ItalicAngle, new PdfNumber(0));
        dicFontDescriptor.Put(PdfName.StemV, new PdfNumber(80));
        dicFontDescriptor.Put(PdfName.Type, PdfName.FontDescriptor);
        dicFontDescriptor.Put(PdfName.FontFile2, streamFontFile.MakeIndirect(document));
        //var fontDescriptorEntry = treeRoot.AddKid(new PdfStructElem(dicFontDescriptor));
        //AddIndirectRef(fontDescriptorEntry, dicFontDescriptor, fileEntry, PdfName.FontFile2);

        // TOUNICODE
        var streamToUnicode = new PdfStream(GetToUnicode());
        //var toUnicodeEntry = treeRoot.AddKid(new PdfStructElem(streamToUnicode));

        // CIDTOGIDMAP
        if (_cidtogidmap == null)
        {
            _cidtogidmap = new byte[KCIDTOGIDMAPSIZE];
            for (int i = 0; i < KCIDTOGIDMAPSIZE; i++)
            {
                _cidtogidmap[i] = ((i % 2) != 0) ? (byte)1 : (byte)0;
            }
        }
        var streamFontFileCIDToGID = new PdfStream(_cidtogidmap);
        //var CIDToGIDMapEntry = treeRoot.AddKid(new PdfStructElem(streamFontFileCIDToGID));

        // SystemInfo
        var dicSystemInfo = new PdfDictionary();
        dicSystemInfo.Put(PdfName.Ordering, new PdfString("Identity"));
        dicSystemInfo.Put(PdfName.Registry, new PdfString("Adobe"));
        dicSystemInfo.Put(PdfName.Supplement, new PdfNumber(0));
        //var SystemInfoEntry = treeRoot.AddKid(new PdfStructElem(dicSystemInfo));
        // CIDFONTTYPE2
        var dicCIDFONTTYPE2 = new PdfDictionary();
        dicCIDFONTTYPE2.Put(PdfName.BaseFont, PDFNAME_GLYPHLESS);
        //dicCIDFONTTYPE2.Put(PdfName.CIDSystemInfo, dicSystemInfo);
        dicCIDFONTTYPE2.Put(PdfName.Subtype, PdfName.CIDFontType2);
        dicCIDFONTTYPE2.Put(PdfName.Type, PdfName.Font);
        dicCIDFONTTYPE2.Put(PdfName.DW, new PdfNumber(1000 / KCHARWIDTH));
        /*
        var WList = new System.Collections.Generic.List<PdfObject>
        {
            new PdfNumber(32),
            new PdfArray(new int[] { 500 })
        };
        dicCIDFONTTYPE2.Put(PdfName.W, new PdfArray(WList));
        */
        dicCIDFONTTYPE2.Put(PdfName.CIDToGIDMap, streamFontFileCIDToGID.MakeIndirect(document));
        dicCIDFONTTYPE2.Put(PdfName.FontDescriptor, dicFontDescriptor.MakeIndirect(document));
        dicCIDFONTTYPE2.Put(PdfName.CIDSystemInfo, dicSystemInfo.MakeIndirect(document));
        //var CIDFONTTYPE2Entry = treeRoot.AddKid(new PdfStructElem(dicCIDFONTTYPE2));
        //AddIndirectRef(CIDFONTTYPE2Entry, dicCIDFONTTYPE2, CIDToGIDMapEntry, PdfName.CIDToGIDMap);
        //AddIndirectRef(CIDFONTTYPE2Entry, dicCIDFONTTYPE2, fontDescriptorEntry, PdfName.FontDescriptor);
        //AddIndirectRef(CIDFONTTYPE2Entry, dicCIDFONTTYPE2, SystemInfoEntry, PdfName.CIDSystemInfo);

        // TYPE0 FONT
        var dicType0Font = new PdfDictionary();
        dicType0Font.Put(PdfName.BaseFont, PDFNAME_GLYPHLESS);
        dicType0Font.Put(PdfName.Encoding, PdfName.IdentityH);
        dicType0Font.Put(PdfName.Subtype, PdfName.Type0);
        dicType0Font.Put(PdfName.Type, PdfName.Font);
        dicType0Font.Put(PdfName.DescendantFonts, new PdfArray(dicCIDFONTTYPE2.MakeIndirect(document)));
        dicType0Font.Put(PdfName.ToUnicode, streamToUnicode.MakeIndirect(document));
        //var type0FontEntry = treeRoot.AddKid(new PdfStructElem(dicType0Font));
        //AddIndirectRef(type0FontEntry, dicType0Font, CIDFONTTYPE2Entry, PdfName.DescendantFonts);
        //AddIndirectRef(type0FontEntry, dicType0Font, toUnicodeEntry, PdfName.ToUnicode);
        //var i = new PdfType0Font(dicType0Font);
        //type0FontEntry.Flush();
        //dicCIDFONTTYPE2.Flush();



        //document.AddFont(f0);
        //var r = new PdfResources(dicCIDFONTTYPE2);
        //return document.GetFont(dicCIDFONTTYPE2);
        //return PdfFontFactory.CreateFont(dicCIDFONTTYPE2);
        return PdfFontFactory.CreateFont(dicType0Font);
    }

    static public byte[] GetToUnicode()
    {
        if (_toUnicode != null) return _toUnicode;
        _toUnicode = System.Text.Encoding.ASCII.GetBytes(TOUNICODE);
        return _toUnicode;
    }

public static void AddIndirectRef(PdfStructElem fromStr, PdfDictionary fromDir, PdfStructElem to, PdfName name)
    {
        //to.Put(name, fromDir);
        //return;
        // must be a better way to add indirect ref but I did not find it
        fromStr.AddRef(to);
        var oIndir = fromDir.Get(PdfName.Ref);
        fromDir.Put(name, oIndir);
        fromDir.Remove(PdfName.Ref);
    }
}
