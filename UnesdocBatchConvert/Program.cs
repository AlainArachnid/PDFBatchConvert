#define MERGE_FONTS
using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.IO;
using iText.Kernel.Pdf;
using iText.Kernel.Pdf.Canvas;
using iText.Kernel.Pdf.Canvas.Parser;
using iText.Kernel.Pdf.Canvas.Parser.Listener;
using iText.Kernel.Pdf.Canvas.Parser.Data;
using iText.Kernel.Utils;
using iText.Kernel.Pdf.Xobject;
using iText.Kernel.Events;
using iText.Kernel.Font;
using iText.Kernel.Geom;
using iText.IO.Font;
using iText.Layout;
using iText.Layout.Element;
using System.Linq;

namespace UnesdocBatchConvert
{
    class Program
    {
        public const String IMAGEMAGICK_FOLDER = @"C:\Program Files\ImageMagick-7.1.1-Q16-HDRI";
        public const String FONT_FILE_GLYPHLESS = @"D:\tmp\pdf.ttf";
        public const String CONVERT_PARAMS = "-grayscale Rec709Luma  -lat 20x20-5% -compress group4";
        public const String INPUT_FOLDER = @"somePathB\OCR";
        public const String OUTPUT_FOLDER_PROD = "otherPath\results";
        public const String OUTPUT_FOLDER_TEST = @"otherPath\testResize";
        public const String INPUT_MODIFIED = @"otherPath\orig.modif";
        public const String TEMP_ROOT = @"D:\work\temp\work";
        public const String PATHICM = @"D:\work\sRGB_CS_profile.icm";
        public const int NB_PROCESS = 6;
        public static bool B_TEST_FOLDER = false;
        public const String TEST_RESTRICT = " AND id=46573";
        public const String TEST_DATE_DONE_PROD = @" AND (dateDone is null or dateDone < '2025-11-10 00:00:00') AND id in (
          22067,
22080,
1143,
19636)";    // to be updated
        public const float SIZE_LIMIT = 32 * 72.0f / 2.54f;
        public const float NEW_SIZE = 29.7f * 72.0f / 2.54f;
        public const int LIMIT_NB_PAGE = 2;
        public const int LIMIT_CHAR_PAGE_BLANK = 10;
        public const bool EXPORT_CREATE_LINKS = true;
        public const String MOUNT_POINT_BINAR_4_UNESDOC = "/mnt/binar.results";

        public static Boolean bTest = false;

        public const string CNX_STRING = "Server=someServer;Database=someDatabase;Trusted_Connection=True";

        public const int MAX_DONE = 99999999;
        public const string SQL_TEST_BIG = "(MaxSizeJP2 > 10000000)";
        static public bool DEBUG_CMAP = false;

        static readonly CWorker[] tabWorker = new CWorker[NB_PROCESS];

        static public System.Data.SqlClient.SqlConnection conn;
        static public int nDone = 0;
        static public bool bStillBig = true;
        static public int lastId = 0;
        static public int lastIdBig = 0;
        static public byte[] icm;

        private static readonly String _patternMatrixString = "0.1 0 0 0.1 0 0";
        private static byte[] _patternMatrixBytes;
        private static int _patternMatrixLength;

        static void Main(string[] args)
        {
            _patternMatrixBytes = System.Text.Encoding.ASCII.GetBytes(_patternMatrixString);
            _patternMatrixLength = _patternMatrixBytes.Length;
            conn = new System.Data.SqlClient.SqlConnection
            {
                ConnectionString = CNX_STRING
            };
            conn.Open();
            int doMode = 16;
            switch (doMode)
            {
                case 1:
                    DoConvert();
                    break;
                case 2:
                    DoScanNASIntoSQL();
                    break;
                case 3:
                    DoTestOneDocAlreadyBinarized();
                    break;
                case 4:
                    DoGetSizeBinarIntoSQL();
                    break;
                case 5:
                    DoGetFullTextIntoSQL();
                    break;
                case 6:
                    DoGetHeightWidthIntoSQL();
                    break;
                case 7:
                    //DoTestCreatePDFWithGlyphless();
                    break;
                case 8:
                    DoGetSizePageIntoSQL();
                    break;
                case 9:
                    DoGetFullTextByPageIntoSQL();
                    break;
                case 10:
                    DoGetSizePageFromOrigIntoSQL();
                    break;
                case 11:
                    DoGetPageContentIntoSQL();
                    break;
                case 12:
                    DoTestOneMergeFonts();
                    break;
                case 13:
                    DoCheckPagesConsecutive();
                    break;
                case 14:
                    DoExportToUNESDOC(5);   // number of batch as argument
                    break;
                case 15:
                    DoGetPagePdfFinalIntoSQL();
                    break;
                case 16:
                    DoGetHeightWidthJP2IntoSQL();
                    break;
                case 50:
                    CPhoto.DoScan();
                    break;
                case 51:
                    CPhoto.DoTestOne();
                    break;
                case 52:
                    CPhoto.DoTestFour();
                    break;
                case 53:
                    CPhoto.DoMergeAll();
                    break;
                case 60:
                    CPortrait.DoScan();
                    break;
                case 61:
                    CPortrait.TestOneScan();
                    break;
                case 62:
                    CPortrait.InitNames();
                    break;
                case 71:
                    DoStatChar();
                    break;
                default:
                    Console.WriteLine("bad doMode " + doMode);
                    break;
            }
#if DEBUG
            Console.WriteLine("Finished         ");
            Console.ReadLine();
#endif
        }

        static public void DoGetPagePdfFinalIntoSQL()
        {
            String sqlGet = "select " + CBinarNASList.FIELD_LIST +
                " from BinarNASList where nbPagePdfFinal is null order by id";
            var lInfo = new List<CBinarNASList>();
            using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
            {
                using (var rs = cmd.ExecuteReader())
                {
                    while (rs.Read())
                    {
                        var oInfo = new CBinarNASList();
                        oInfo.LoadFromRS(rs);
                        lInfo.Add(oInfo);
                    }
                }
            }
            while (lInfo.Count > 0)
            {
                using (var trans = conn.BeginTransaction())
                {
                    int nDone = 0;
                    var lInfoDone = new List<CBinarNASList>();
                    foreach (var oInfo in lInfo)
                    {
                        String sSubpath;
                        if (oInfo.manual)
                        {
                            sSubpath = @"\manual\" + oInfo.id.ToString() + ".pdf";
                        }
                        else
                        {
                            sSubpath = oInfo.target.Replace(@"otherPath\results", "");
                        }
                        String sTarget = @"somePath\binar.results" + sSubpath;
                        int nbp = 0;
                        if (!System.IO.File.Exists(sTarget))
                        {
                            Console.WriteLine("not found " + sTarget);
                        }
                        else
                        {
                            using (var Pdfreader = new PdfReader(sTarget))
                            using (PdfDocument document = new PdfDocument(Pdfreader))
                            {
                                nbp = document.GetNumberOfPages();
                            }
                        }
                        using (var cmd = new System.Data.SqlClient.SqlCommand("update BinarNASList set nbPagePdfFinal=@nbp where id=@id", Program.conn, trans))
                        {
                            cmd.Parameters.AddWithValue("@nbp", nbp);
                            cmd.Parameters.AddWithValue("@id", oInfo.id);
                            cmd.ExecuteNonQuery();
                        }
                        Console.Write("id=" + oInfo.id + "      \r");
                        lInfoDone.Add(oInfo);
                        nDone += 1;
                        if (nDone >= 100) break;
                    }
                    trans.Commit();
                    foreach (var oInfo in lInfoDone) lInfo.Remove(oInfo);
                }
            }
        }

        static public void DoExportToUNESDOC(int batch)
        {
            //Console.WriteLine("Add managment of manual PDF"); if (Math.Min(1, 1) != 1) return;
            String sqlGet = "select " + CBinarNASList.FIELD_LIST +
                " from BinarNASList where BatchToUNESDOC=@b";
            String targetFolder = @"otherPath\BatchUNESDOC" + batch.ToString("00");
            using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
            {
                cmd.Parameters.AddWithValue("@b", batch);
                using (var rs = cmd.ExecuteReader())
                {
                    if (!rs.HasRows) return;
                    using (var csvFile = System.IO.File.CreateText(targetFolder + @"\Batch" + batch.ToString("00") + ".csv"))
                    {
                        csvFile.Write("\"Seqno\",\"Language\",\"PDFName\"\n");
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            if (!oInfo.Seqno.HasValue || oInfo.Seqno == 0)
                            {
                                Console.WriteLine("ID=" + oInfo.id.ToString("00000") + " no seqno");
                                continue;
                            }
                            if (String.IsNullOrEmpty(oInfo.Lang))
                            {
                                Console.WriteLine("ID=" + oInfo.id.ToString("00000") + " no lang");
                                continue;
                            }
                            String sLink, sSubpath;
                            if (oInfo.manual)
                            {
                                sSubpath = @"\manual\" + oInfo.id.ToString() + ".pdf";
                            }
                            else
                            {
                                sSubpath = oInfo.target.Replace(@"otherPath\unesco.binar.results", "");
                            }
                            String sTranslatedLang = oInfo.Lang.ToLower();
                            if (sTranslatedLang == "plu") sTranslatedLang = "qaa";
                            if (EXPORT_CREATE_LINKS || Math.Max(1, 1) == 0)
                            {
                                sLink = ((int)(oInfo.Seqno)).ToString("000000") + sTranslatedLang + ".pdf";
                                String sLinkFull = targetFolder + @"\" + sLink;
                                //String sTarget = oInfo.target;
                                String sTarget = @"somePath\results" + sSubpath;
                                if (!CreateSymbolicLink(sLinkFull, sTarget, SymbolicLink.AllowUnpriv))
                                {
                                    int lastError = System.Runtime.InteropServices.Marshal.GetLastWin32Error();
                                    if (lastError == 0x522)
                                    {
                                        Console.WriteLine("==================");
                                        Console.WriteLine("Must run as admin!");
                                        Console.WriteLine("==================");
                                        return;
                                    }
                                    if (lastError == 0xB7)
                                    {
                                        Console.WriteLine("ID=" + oInfo.id.ToString("00000") + " symlink " + sLinkFull + " already exists");
                                    }
                                    else
                                    {
                                        Console.WriteLine("ID=" + oInfo.id.ToString("00000") + " error 0x" + lastError.ToString("X") + " symlink " + sLinkFull + " to " + sTarget);
                                    }
                                    continue;
                                }
                            }
                            else
                            {
                                sLink = MOUNT_POINT_BINAR_4_UNESDOC + sSubpath.Replace(@"\", "/");
                            }
                            csvFile.Write("\"" + ((int)oInfo.Seqno).ToString("000000") + "\",\"" + sTranslatedLang + "\",\"" + sLink + "\"\n");
                        }
                    }
                }
            }
        }

        [System.Runtime.InteropServices.DllImport("kernel32.dll")]
        static extern bool CreateSymbolicLink(
        string lpSymlinkFileName, string lpTargetFileName, SymbolicLink dwFlags);

        enum SymbolicLink
        {
            File = 0,
            Directory = 1,
            AllowUnpriv = 2
        }

        static public void DoStatChar()
        {
            String sqlget = "select top 300 id, page, textPDF from BinarPage where nbDistinctChar is null order by id, page";
            for (; ; )
            {
                var sqlupdate = new System.Text.StringBuilder();
                int iParam = 1;
                using (var cmdupdate = new System.Data.SqlClient.SqlCommand(null, Program.conn))
                {
                    int id = 0;
                    using (var cmdget = new System.Data.SqlClient.SqlCommand(sqlget, Program.conn))
                    {
                        using (var rs = cmdget.ExecuteReader())
                        {
                            while (rs.Read())
                            {
                                id = rs.GetInt32(0);
                                var page = rs.GetInt32(1);
                                var text = rs.GetString(2);
                                var iParamS = iParam.ToString();
                                int nbCyrilic = 0;
                                int nbArabic = 0;
                                int nbChinese = 0;

                                Dictionary<int, Object> dicBig = null;
                                byte[] tabExists = new byte[0x10000];
                                var sReader = new StringReader(text);
                                int iMaxChar = 0;
                                for (; ; )
                                {
                                    int iChar = sReader.Read();
                                    if (iChar < 0) break;
                                    if (iChar >= 0x10000)
                                    {
                                        if (dicBig == null) dicBig = new Dictionary<int, Object>();
                                        Console.WriteLine("id=" + id.ToString() + ", page=" + page.ToString() + " big char " + iChar.ToString());
                                        dicBig[iChar] = null;
                                    }
                                    else
                                    {
                                        tabExists[iChar] = 1;
                                        if (iChar > iMaxChar) iMaxChar = iChar;
                                    }
                                    if (iChar >= 0x400 && iChar <= 0x42F) nbCyrilic++;
                                    if (iChar >= 0x600 && iChar <= 0x6FF) nbArabic++;
                                    if (iChar >= 0x3300 && iChar <= 0x9FFF) nbChinese++;
                                }
                                int nbDistinctChar = 0;
                                for (int iChar = 0; iChar <= iMaxChar; iChar++) if (tabExists[iChar] > 0) nbDistinctChar++;
                                if (dicBig != null) nbDistinctChar += dicBig.Count;

                                sqlupdate.Append("update BinarPage set nbDistinctChar=@x" + iParamS + "x");
                                sqlupdate.Append(", nbCyrillic=@nr" + iParamS + "y");
                                sqlupdate.Append(", nbArabic=@na" + iParamS + "y");
                                sqlupdate.Append(", nbChinese=@nc" + iParamS + "y");
                                sqlupdate.Append(" where id=@i" + iParamS + "i");
                                sqlupdate.Append(" and page=@p" + iParamS + "p; ");
                                cmdupdate.Parameters.AddWithValue("@x" + iParamS + "x", nbDistinctChar);
                                if (nbCyrilic > 0)
                                    cmdupdate.Parameters.AddWithValue("@nr" + iParamS + "y", nbCyrilic);
                                else
                                    cmdupdate.Parameters.AddWithValue("@nr" + iParamS + "y", DBNull.Value);
                                if (nbArabic > 0)
                                    cmdupdate.Parameters.AddWithValue("@na" + iParamS + "y", nbArabic);
                                else
                                    cmdupdate.Parameters.AddWithValue("@na" + iParamS + "y", DBNull.Value);
                                if (nbChinese > 0)
                                    cmdupdate.Parameters.AddWithValue("@nc" + iParamS + "y", nbChinese);
                                else
                                    cmdupdate.Parameters.AddWithValue("@nc" + iParamS + "y", DBNull.Value);
                                cmdupdate.Parameters.AddWithValue("@i" + iParamS + "i", id);
                                cmdupdate.Parameters.AddWithValue("@p" + iParamS + "p", page);
                                iParam++;
                            }
                        }
                    }
                    var sql = sqlupdate.ToString();
                    if (sql == "") break;
                    Console.Write("update last id " + id.ToString() + "     \r");
                    cmdupdate.CommandText = sqlupdate.ToString();
                    //Console.WriteLine(cmdupdate.CommandText);
                    //Console.WriteLine("nb params=" + cmdupdate.Parameters.Count);
                    cmdupdate.ExecuteNonQuery();
                    //return;
                }
            }
        }

        static public void DoTestOneMergeFonts()
        {
            String[] tabOld = new string[] { @"some.pdf" };
            String[] tabNew = new string[] { @"result.pdf" };
            for (int i = 0; i < tabOld.Length; i++)
            {
                String sRet = DoMergeFonts(tabOld[i], tabNew[i]);
                if (sRet == null) sRet = "OK";
                Console.WriteLine(tabOld[i] + " " + sRet);
            }
        }

        static public String DoMergeFonts(String pathOld, String pathNew)
        {
            var w = new WriterProperties();
            w.AddXmpMetadata();
            using (var reader = new PdfReader(pathOld))
            using (var writer = new PdfWriter(pathNew, w))
            using (var doc = new iText.Pdfa.PdfADocument(reader, writer))
            {
                //var conformanceLevel = doc.GetConformanceLevel();
                //var xmp = doc.GetXmpMetadata();
                //doc.SetXmpMetadata();
                DoMergeFontsDoc(doc, 0);

                //doc.SetFlushUnusedObjects(true);
                doc.Close();
            }

            return null;
        }

        static public String DoMergeFontsDoc(PdfDocument doc, int id)
        {
            // "branch" all pages to basefont of first page
            // remove basefont of other pages
            // remove /CIDSet of basefont
            // remove /W of basefont
            // return null if ok, else error msg
            const int PAGE_TRACE = -1;
            int nbPage = doc.GetNumberOfPages();
            PdfDictionary uniqueFont = null;
            byte[] tabGlyphUsed = new byte[0x10000];
            for (int iPage = 1; iPage <= nbPage; iPage++)
            {
                var oPage = doc.GetPage(iPage);
                var tabResources = oPage.GetResources();
                var dicFonts = tabResources.GetResource(PdfName.Font);
                //if (dicFonts == null && iPage == 1) return "Page " + iPage.ToString() + " contains no font, should contain one";
                if (dicFonts == null) continue; // skip page
                if (dicFonts.Size() != 1) return "Page " + iPage.ToString() + " contains " + dicFonts.Size().ToString() + " fonts, should contain only one";
                var eltFont = dicFonts.Values().First();
                if (eltFont.GetObjectType() != PdfObject.DICTIONARY) return "Page " + iPage.ToString() + ", font of type " + eltFont.GetObjectType() + ", not a dictionary";
                var dicFont = (PdfDictionary)eltFont;
                if (iPage == PAGE_TRACE)
                {
                    foreach (var kv in dicFont.EntrySet()) Console.WriteLine("Page " + iPage + " " + kv.Key + " " + kv.Value.GetObjectType());
                    Console.WriteLine("");
                }

                String sError = GetUniquePdfDictionary(dicFont.Get(PdfName.DescendantFonts), out PdfDictionary descendantFontD);
                if (sError != null) return "Page " + iPage.ToString() + " DescendantFont " + sError;
                if (iPage == PAGE_TRACE)
                {
                    foreach (var kv in descendantFontD.EntrySet()) Console.WriteLine("Page " + iPage + " " + kv.Key + " " + kv.Value.GetObjectType());
                    Console.WriteLine("");
                }

                sError = GetUniquePdfDictionary(descendantFontD.Get(PdfName.FontDescriptor), out PdfDictionary FontDescriptorD);
                if (sError != null) return "Page " + iPage.ToString() + " FontDescriptor " + sError;
                if (iPage == PAGE_TRACE)
                {
                    foreach (var kv in FontDescriptorD.EntrySet()) Console.WriteLine("Page " + iPage + " " + kv.Key + " " + kv.Value.GetObjectType());
                    Console.WriteLine("");
                }

                // Remove CIDSet, W
                FontDescriptorD.Remove(PdfName.CIDSet);
                descendantFontD.Remove(PdfName.W);
                // Force CIDToGIDMap to Identity    => fail PDF/A2b, not necessary, actually contains 0x100000 (!) bits "0"
                //descendantFontD.Put(PdfName.CIDToGIDMap, PdfName.Identity);

                if (DEBUG_CMAP)
                {
                    Console.WriteLine("DEBUG_CMAP active");
                    byte[] bValTrace = ((PdfStream)descendantFontD.Get(PdfName.CIDToGIDMap)).GetBytes();
                    String FileNameTrace = @"d:\tmp\CIDToGIDMap" + id.ToString("000000") + ".page" + iPage + ".txt";
                    System.IO.File.WriteAllBytes(FileNameTrace, bValTrace);
                }

                // Force ToUnicode to /Identity => does not work (erreor when opening PDF in PDFReader, missing CMap //dicFont.Put(PdfName.ToUnicode, PdfName.Identity);
                // Force ToUnicode to CMap Identity => does not work for non ASCII chars
#if MERGE_FONTS

                byte[] bVal = ((PdfStream)dicFont.Get(PdfName.ToUnicode)).GetBytes();
                if (DEBUG_CMAP)
                {
                    String FileNameCMap = @"d:\tmp\CMAP" + id.ToString("000000") + ".page" + iPage + ".txt";
                    System.IO.File.WriteAllBytes(FileNameCMap, bVal);
                }

                // Same Font for all pages
                if (uniqueFont == null)
                {
                    uniqueFont = dicFont;
                }
                else
                {
                    dicFonts.Put(dicFonts.KeySet().First(), uniqueFont);
                }
                //PdfType0Font font = (PdfType0Font)PdfFontFactory.CreateFont(FONT_FILE_GLYPHLESS, PdfEncodings.IDENTITY_H, PdfFontFactory.EmbeddingStrategy.PREFER_EMBEDDED, false);
                var font = (PdfType0Font)PdfFontFactory.CreateFont(dicFont);
                int nbGlyph = 0;
                for (int iUnicode = 0; iUnicode < 0x10000; iUnicode++) if (font.ContainsGlyph(iUnicode))
                    {
                        tabGlyphUsed[iUnicode] = 1;
                        nbGlyph++;
                    }
                if (DEBUG_CMAP) Console.WriteLine("id=" + id + ", page=" + iPage + ", nbGlyph=" + nbGlyph);
#endif
            }
#if MERGE_FONTS
            const String CMAP_START = @"/CIDInit /ProcSet findresource begin
12 dict begin
begincmap
/CMapType 2 def
/CMapName/R20 def
1 begincodespacerange
<0000><ffff>
endcodespacerange
";
            const String CMAP_END = @"endcmap
CMapName currentdict /CMap defineresource pop
end end
";
            var cmapUnique = new System.Text.StringBuilder();
            cmapUnique.Append(CMAP_START);
            var listRange = new List<String>();
            int nbGlyphTotal = 0;
            bool inRange = false;
            int startRange = 0;
            //int nbRange = 0;
            for (int iUnicode = 0; iUnicode < 0x10000; iUnicode++)
            {
                if (tabGlyphUsed[iUnicode] > 0)
                {
                    nbGlyphTotal++;
                    if (inRange) continue;
                    startRange = iUnicode;
                    inRange = true;
                }
                else
                {
                    if (!inRange) continue;
                    // add range from startRange to iUnicode-1
                    String sStart = "<" + startRange.ToString("x4") + ">";
                    listRange.Add(sStart + "<" + (iUnicode - 1).ToString("x4") + ">" + sStart);
                    //nbRange++;
                    inRange = false;
                }
            }
            if (inRange)
            {
                String sStart = "<" + startRange.ToString("x4") + ">";
                listRange.Add(sStart + "<ffff>" + sStart);
                //nbRange++;
            }
            for (int iRange = 0; iRange < listRange.Count; iRange += 100)
            {
                int thisNb = 100;
                if (listRange.Count < iRange + 100) thisNb = listRange.Count - iRange;
                cmapUnique.Append(thisNb + " beginbfrange\n");
                for (int iRange2 = 0; iRange2 < thisNb; iRange2++)
                {
                    cmapUnique.Append(listRange[iRange + iRange2]);
                    cmapUnique.Append("\n");
                }
                cmapUnique.Append("endbfrange\n");
            }
            cmapUnique.Append(CMAP_END);
            var streamIdentCMap = new PdfStream(System.Text.Encoding.ASCII.GetBytes(cmapUnique.ToString()));
            //Console.WriteLine("skip set patched font");
            if (uniqueFont == null)
            {
                Program.LogErrorSQL("font issue", "Not unique font in document", null, null, 0, null, 0, id);
            }
            else
            {
                uniqueFont.Put(PdfName.ToUnicode, streamIdentCMap);
            }
            if (DEBUG_CMAP)
            {
                Console.WriteLine("id=" + id + ", final, nbGlyph=" + nbGlyphTotal);
                byte[] bVal = ((PdfStream)uniqueFont.Get(PdfName.ToUnicode)).GetBytes();
                String FileNameCMap = @"d:\tmp\CMAP" + id.ToString("000000") + ".final.txt";
                System.IO.File.WriteAllBytes(FileNameCMap, bVal);
            }
#endif
            return null;
        }

        static public void InitICM()
        {
            icm = System.IO.File.ReadAllBytes(Program.PATHICM);
        }

        static public String GetUniquePdfDictionary(PdfObject oIn, out PdfDictionary dicOut)
        {
            dicOut = null;
            if (oIn == null) return "is null";
            switch (oIn.GetObjectType())
            {
                case PdfObject.ARRAY:
                    var oInA = (PdfArray)oIn;
                    if (oInA.Size() != 1) return "array of size " + oInA.Size() + ", should be 1";
                    return GetUniquePdfDictionary(oInA.Get(0), out dicOut);
                case PdfObject.INDIRECT_REFERENCE:
                    var oInP = (PdfIndirectReference)oIn;
                    return GetUniquePdfDictionary(oInP.GetRefersTo(), out dicOut);
                case PdfObject.DICTIONARY:
                    dicOut = (PdfDictionary)oIn;
                    return null;
                default:
                    return "not Array or Indirect, type=" + oIn.GetObjectType();
            }
        }

        static public void DoResizePages()
        {

            String sqlGet = "select top 100 " + CBinarNASList.FIELD_LIST +
                " from BinarNASList where dateDone is not null and [bigHeight] > " + SIZE_LIMIT;
            String sqlUpdate = "update BinarNASList set dateResize = GETDATE() where id=@id";
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                        }
                    }
                }
                foreach (var oInfo in lDoc)
                {
                    bool bDone = DoResize(oInfo.target);
                    if (bDone)
                    {
                        using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn))
                        {
                            cmd.Parameters.AddWithValue("@id", oInfo.id);
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                //return;
            }
        }

        static public bool DoResize(String path)
        {
            using (PdfDocument document = new PdfDocument(new PdfReader(path)))
            {
                return Program.DoResizeDocument(document);
            }
        }

        static public bool DoResizeDocument(PdfDocument document)
        {
            bool bDone = false;
            var pageNumbers = document.GetNumberOfPages();
            ScaleDownEventHandler previousHandler = null;
            for (int iPage = 1; iPage <= pageNumbers; iPage++)
            {
                var oPage = document.GetPage(iPage);
                var oRect = oPage.GetPageSize();
                float w = oRect.GetWidth();
                float h = oRect.GetHeight();
                float percentage = Math.Min(w / SIZE_LIMIT, h / SIZE_LIMIT);
                if (percentage >= 1.0f) continue;
                if (w > SIZE_LIMIT)
                {
                    if (h > w) percentage = NEW_SIZE / h;
                    else percentage = NEW_SIZE / w;
                }
                else
                {
                    if (h <= SIZE_LIMIT) continue;
                    percentage = NEW_SIZE / h;
                }
                ScaleDownEventHandler eventHandler = new ScaleDownEventHandler(percentage);
                if (previousHandler != null) document.RemoveEventHandler(PdfDocumentEvent.START_PAGE, previousHandler);
                document.AddEventHandler(PdfDocumentEvent.START_PAGE, eventHandler);
                eventHandler.SetPageDict(oPage.GetPdfObject());

                // Copy and paste scaled iPage content as formXObject
                PdfFormXObject xPage = oPage.CopyAsFormXObject(document);
                PdfCanvas canvas = new PdfCanvas(document.AddNewPage());
                canvas.AddXObjectWithTransformationMatrix(xPage, percentage, 0f, 0f, percentage, 0f, 0f);
                bDone = true;
            }
            return bDone;
        }

        private class ScaleDownEventHandler : IEventHandler
        {
            protected float scale = 1;
            protected PdfDictionary pageDict;

            public ScaleDownEventHandler(float scale)
            {
                this.scale = scale;
            }

            public void SetPageDict(PdfDictionary pageDict)
            {
                this.pageDict = pageDict;
            }

            public void HandleEvent(Event currentEvent)
            {
                PdfDocumentEvent docEvent = (PdfDocumentEvent)currentEvent;
                PdfPage page = docEvent.GetPage();

                page.Put(PdfName.Rotate, pageDict.GetAsNumber(PdfName.Rotate));

                // The MediaBox value defines the full size of the page.
                ScaleDown(page, pageDict, PdfName.MediaBox, scale);

                // The CropBox value defines the visible size of the page.
                ScaleDown(page, pageDict, PdfName.CropBox, scale);
            }

            protected void ScaleDown(PdfPage destPage, PdfDictionary pageDictSrc, PdfName box, float scale)
            {
                PdfArray original = pageDictSrc.GetAsArray(box);
                if (original != null)
                {
                    float width = original.GetAsNumber(2).FloatValue() - original.GetAsNumber(0).FloatValue();
                    float height = original.GetAsNumber(3).FloatValue() - original.GetAsNumber(1).FloatValue();

                    PdfArray result = new PdfArray
                    {
                        new PdfNumber(0),
                        new PdfNumber(0),
                        new PdfNumber(width * scale),
                        new PdfNumber(height * scale)
                    };
                    destPage.Put(box, result);
                }
            }
        }

        static public void DoSizeBlank()
        {
            String sqlGet = "select top 100 " + CBinarNASList.FIELD_LIST +
                " from BinarNASList where id > @id order by id";
            String sqlUpdate = "update BinarNASList set [nbBlankPage]=@nb" +
                ", [sizeSmallestBlankPage]=@ss, [sizeBigestBlankPage]=@sb" +
                ", [pageSmallestBlank]=@ps, [pageBigestBlank]=@pb" +
                ", [firstPage5Char]=@f5, [lastPage5Char]=@l5, [nbPage5Char]=@n5" +
                " where id=@id";
            int lastId = 0;
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    cmd.Parameters.AddWithValue("@id", lastId);
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                        }
                    }
                }
                foreach (var oInfo in lDoc)
                {
                    lastId = oInfo.id;
                    oInfo.ResetBlankInfo();
                    using (PdfDocument document = new PdfDocument(new PdfReader(oInfo.target)))
                    {
                        var pageNumbers = document.GetNumberOfPages();
                        System.IO.FileInfo[] tabJP2Files = null;
                        for (int page = 1; page <= pageNumbers; page++)
                        {
                            //new LocationTextExtractionStrategy creates a new text extraction renderer
                            LocationTextExtractionStrategy strategy = new LocationTextExtractionStrategy();
                            PdfCanvasProcessor parser = new PdfCanvasProcessor(strategy);
                            parser.ProcessPageContent(document.GetPage(page));
                            String s = strategy.GetResultantText().Trim();
                            int nbChar = s.Length;
                            /*
                            String firstChar = "";
                            if (nbChar >= 1) firstChar = " " + System.Text.Encoding.UTF8.GetBytes(s)[0];
                            if (nbChar <= 5) Console.WriteLine(oInfo.id + " " + oInfo.folder + " page " + page + " nbChar=" + nbChar + firstChar);
                            */
                            if (nbChar == 0)
                            {
                                if (tabJP2Files == null)
                                {
                                    var oDirSource = new System.IO.DirectoryInfo(oInfo.folder);
                                    tabJP2Files = oDirSource.GetFiles(oInfo.basename + "_*.jp2");
                                }
                                if (page > tabJP2Files.Length)
                                {
                                    Console.WriteLine("*** page de JP2 absente " + oInfo.folder + " page " + page.ToString());
                                    continue;
                                }
                                long sJP2 = tabJP2Files[page - 1].Length;

                                if (oInfo.nbBlankPage.HasValue)
                                {
                                    oInfo.nbBlankPage++;
                                    if (oInfo.sizeSmallestBlankPage > sJP2)
                                    {
                                        oInfo.sizeSmallestBlankPage = sJP2;
                                        oInfo.pageSmallestBlank = page;
                                    }
                                    if (oInfo.sizeBigestBlankPage < sJP2)
                                    {
                                        oInfo.sizeBigestBlankPage = sJP2;
                                        oInfo.pageBigestBlank = page;
                                    }
                                }
                                else
                                {
                                    oInfo.nbBlankPage = 1;
                                    oInfo.sizeSmallestBlankPage = sJP2;
                                    oInfo.sizeBigestBlankPage = sJP2;
                                    oInfo.pageSmallestBlank = page;
                                    oInfo.pageBigestBlank = page;
                                }
                            }
                            else if (nbChar <= 5)
                            {
                                if (oInfo.firstPage5Char == 0)
                                {
                                    oInfo.firstPage5Char = page;
                                    oInfo.lastPage5Char = page;
                                    oInfo.nbPage5Char = 1;
                                }
                                else
                                {
                                    if (oInfo.firstPage5Char > page) oInfo.firstPage5Char = page;
                                    if (oInfo.lastPage5Char < page) oInfo.lastPage5Char = page;
                                    oInfo.nbPage5Char++;
                                }
                            }
                        }
                        //Console.WriteLine(pageText.ToString());

                    }
                    using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn))
                    {
                        cmd.Parameters.AddWithValue("@nb", NullIsDBNull(oInfo.nbBlankPage));
                        cmd.Parameters.AddWithValue("@ss", ZeroIsDBNullLong(oInfo.sizeSmallestBlankPage));
                        cmd.Parameters.AddWithValue("@sb", ZeroIsDBNullLong(oInfo.sizeBigestBlankPage));
                        cmd.Parameters.AddWithValue("@ps", ZeroIsDBNull(oInfo.pageSmallestBlank));
                        cmd.Parameters.AddWithValue("@pb", ZeroIsDBNull(oInfo.pageBigestBlank));
                        cmd.Parameters.AddWithValue("@f5", ZeroIsDBNull(oInfo.firstPage5Char));
                        cmd.Parameters.AddWithValue("@l5", ZeroIsDBNull(oInfo.lastPage5Char));
                        cmd.Parameters.AddWithValue("@n5", ZeroIsDBNull(oInfo.nbPage5Char));
                        cmd.Parameters.AddWithValue("@id", oInfo.id);
                        cmd.ExecuteNonQuery();
                    }
                }
                return;
            }
        }

        static public void DoGetSizePageFromOrigIntoSQL()
        {
            int lastId = 0;
            String sqlFirst = "select top 1 id from BinarPage where width=height order by id";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sqlFirst, Program.conn))
            {
                lastId = ((int)cmd.ExecuteScalar()) - 1;
            }
            String sqlGet = "select top 100 l." + CBinarNASList.FIELD_LIST +
                " from BinarNASList l where l.id > @lastid order by l.id";
            String sqlUpdate = "update [dbo].[BinarPage] set width=@w, height=@h where id=@id and page=@page";
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    cmd.Parameters.AddWithValue("@lastid", lastId);
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                            lastId = oInfo.id;
                        }
                    }
                }
                foreach (var oInfo in lDoc)
                {
                    using (var trans = conn.BeginTransaction())
                    {
                        for (int iPage = 1; iPage <= oInfo.nbPageJP2; iPage++)
                        {
                            iText.Kernel.Geom.Rectangle pageSize;
                            String pathPDF = oInfo.path + @"\" + oInfo.basename + "_" + iPage.ToString("0000") + ".pdf";
                            if (!System.IO.File.Exists(pathPDF))
                            {
                                Console.WriteLine("not found " + pathPDF);
                                continue;
                            }
                            using (var Pdfreader = new PdfReader(pathPDF))
                            using (PdfDocument document = new PdfDocument(Pdfreader))
                            {
                                pageSize = document.GetPage(1).GetPageSizeWithRotation();
                            }
                            using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn, trans))
                            {
                                cmd.Parameters.AddWithValue("@id", oInfo.id);
                                cmd.Parameters.AddWithValue("@page", iPage);
                                cmd.Parameters.AddWithValue("@w", (int)pageSize.GetWidth());
                                cmd.Parameters.AddWithValue("@h", (int)pageSize.GetHeight());
                                cmd.ExecuteNonQuery();
                            }
                        }
                        Console.Write("id=" + oInfo.id + "      \r");
                        trans.Commit();
                    }
                }
            }
        }

        static public void DoGetSizePageIntoSQL()
        {
            String sqlGet = "select top 100 l." + CBinarNASList.FIELD_LIST +
                " from BinarNASList l left outer join [BinarPage] p on p.id=l.id where p.id is null" +
                " and dateDone is not null and l.id > @lastid order by l.id";

            String sqlUpdate = "insert into [dbo].[BinarPage]([id], [page], [sizePJ2], [nbChar], [nbCharTrim], width, height, textPDF)" +
                    "values (@id, @page, @s, @nc, @nt, @w, @h, @t)";
            int lastId = 0;
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    cmd.Parameters.AddWithValue("@lastid", lastId);
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                        }
                    }
                }
                foreach (var oInfo in lDoc)
                {
                    try
                    {
                        using (var trans = conn.BeginTransaction())
                        {
                            lastId = oInfo.id;
                            using (PdfDocument document = new PdfDocument(new PdfReader(oInfo.target)))
                            {
                                var pageNumbers = document.GetNumberOfPages();
                                var oDirSource = new System.IO.DirectoryInfo(oInfo.folder);
                                var tabJP2Files = oDirSource.GetFiles(oInfo.basename + "_*.jp2");
                                for (int page = 1; page <= pageNumbers; page++)
                                {
                                    if (page > tabJP2Files.Length)
                                    {
                                        Console.WriteLine("*** page de JP2 absente " + oInfo.folder + " page " + page.ToString());
                                        continue;
                                    }
                                    long sJP2 = tabJP2Files[page - 1].Length;
                                    //new LocationTextExtractionStrategy creates a new text extraction renderer
                                    LocationTextExtractionStrategy strategy = new LocationTextExtractionStrategy();
                                    PdfCanvasProcessor parser = new PdfCanvasProcessor(strategy);
                                    var oPage = document.GetPage(page);
                                    parser.ProcessPageContent(oPage);
                                    String s = strategy.GetResultantText();
                                    var pageSize = oPage.GetPageSizeWithRotation();
                                    using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn, trans))
                                    {
                                        cmd.Parameters.AddWithValue("@id", oInfo.id);
                                        cmd.Parameters.AddWithValue("@page", page);
                                        cmd.Parameters.AddWithValue("@s", sJP2);
                                        cmd.Parameters.AddWithValue("@nc", s.Length);
                                        cmd.Parameters.AddWithValue("@nt", s.Trim().Length);
                                        if (s.Length > 0)
                                            cmd.Parameters.AddWithValue("@t", s);
                                        else
                                            cmd.Parameters.AddWithValue("@t", DBNull.Value);
                                        cmd.Parameters.AddWithValue("@w", (int)pageSize.GetWidth());
                                        cmd.Parameters.AddWithValue("@h", (int)pageSize.GetHeight());
                                        cmd.ExecuteNonQuery();
                                    }
                                }
                            }
                            trans.Commit();
                        }
                    }
                    catch (Exception e)
                    {
                        //Console.WriteLine("Exception " + e.StackTrace);
                        Program.LogErrorSQL("DoGetSizePageIntoSQL", e.Message + "\n" + e.StackTrace, oInfo.folder, oInfo.box, oInfo.Seqno ?? 0, oInfo.Lang, oInfo.Joker ?? 0, oInfo.id);
                    }
                }
                //return;
            }
        }

        static public void DoCheckPagesConsecutive()
        {
            String sqlGet = "select top 100 l." + CBinarNASList.FIELD_LIST +
                " from BinarNASList l where l.id > @lastid order by l.id";

            int lastId = 0;
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    cmd.Parameters.AddWithValue("@lastid", lastId);
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                            lastId = oInfo.id;
                        }
                    }
                }
                Console.Write("lastid=" + lastId + "       \r");
                var iPage = 1;
                foreach (var oInfo in lDoc)
                {
                    var oFolder = new DirectoryInfo(oInfo.folder);
                    var lPDF = oFolder.GetFiles(oInfo.basename + "_*.pdf");
                    var lJP2 = oFolder.GetFiles(oInfo.basename + "_*.jp2");
                    lPDF.OrderBy(Name => Name);
                    lJP2.OrderBy(Name => Name);
                    var tabPDF = lPDF.ToArray();
                    var tabJP2 = lJP2.ToArray();
                    var nbPDF = tabPDF.Length;
                    var nbJP2 = tabJP2.Length;
                    if (nbPDF != nbJP2)
                        Console.WriteLine("id=" + oInfo.id.ToString("00000") + (oInfo.nbMultipage > 0 ? "X" : " ") + " not same no " + oInfo.basename + " : " + nbPDF + " PDF, " + nbJP2 + " JP2");
                    for (iPage = 1; iPage <= nbPDF; iPage++)
                    {
                        if (tabPDF[iPage - 1].Name != oInfo.basename + "_" + iPage.ToString("0000") + ".pdf")
                        {
                            Console.WriteLine("id=" + oInfo.id.ToString("00000") + (oInfo.nbMultipage > 0 ? "X" : " ") + " bad pdf " + oInfo.basename + " iPage=" + iPage + " file=" + tabPDF[iPage - 1].Name);
                            //return;
                            break;
                        }
                        if (tabJP2[iPage - 1].Name != oInfo.basename + "_" + iPage.ToString("0000") + ".jp2")
                        {
                            Console.WriteLine("id=" + oInfo.id.ToString("00000") + (oInfo.nbMultipage > 0 ? "X" : " ") + " bad jp2 " + oInfo.basename + " iPage=" + iPage + " file=" + tabJP2[iPage - 1].Name);
                            break;
                        }
                    }
                }
            }
        }

        static public void DoGetPageContentIntoSQL()
        {
            String sqlGet = "select top 100 l." + CBinarNASList.FIELD_LIST +
                " from BinarNASList l where exists(select * from BinarPage p where p.id=l.id and nbContent is null)" +
                " order by id";
            String sqlUpdate = "update BinarPage set nbContent=@n, StartFirstContent=@t where id=@id and [page]=@page";
            Console.WriteLine("start " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                        }
                    }
                }
                foreach (var oInfo in lDoc)
                {
                    Console.Write("id " + oInfo.id + " " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "      \r");
                    using (var trans = Program.conn.BeginTransaction())
                    {
                        if (!System.IO.Directory.Exists(oInfo.path))
                        {
                            Console.WriteLine("no folder " + oInfo.path + " ");
                            continue;
                        }
                        for (int page = 1; page <= oInfo.nbPagePdfOrig; page++)
                        {
                            String pathPDF = oInfo.path + @"\" + oInfo.basename + "_" + page.ToString("0000") + ".pdf";
                            if (!System.IO.File.Exists(pathPDF))
                            {
                                Console.WriteLine("no file " + pathPDF + " ");
                                continue;
                            }
                            using (var oReader = new PdfReader(pathPDF))
                            using (PdfDocument document = new PdfDocument(oReader))
                            {
                                var oPage = document.GetPage(1);
                                var n = oPage.GetContentStreamCount();
                                String s = null;
                                if (n > 0)
                                {
                                    s = System.Text.Encoding.ASCII.GetString(oPage.GetContentStream(0).GetBytes()).Substring(0, 40);
                                }
                                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn, trans))
                                {
                                    cmd.Parameters.AddWithValue("@n", n);
                                    if (s == null)
                                        cmd.Parameters.AddWithValue("@t", DBNull.Value);
                                    else
                                        cmd.Parameters.AddWithValue("@t", s);
                                    cmd.Parameters.AddWithValue("@id", oInfo.id);
                                    cmd.Parameters.AddWithValue("@page", page);
                                    var affected = cmd.ExecuteNonQuery();
                                    if (affected != 1) Console.WriteLine("id=" + oInfo.id + " page=" + page + " => " + affected + " row(s) affected");
                                }
                            }
                            //Console.WriteLine(pageText.ToString());
                        }
                        trans.Commit();
                    }
                }
                //return;
            }
        }

        static public void DoGetFullTextByPageIntoSQL()
        {
            String sqlGet = "select top 100 " + CBinarNASList.FIELD_LIST +
                " from BinarNASList l where id > @id and DateDone is not null" +
                " and exists(select * from BinarPage p where p.id=l.id and textPDF is null)" +
                " order by id";
            String sqlUpdate = "update BinarPage set textPDF=@t where id=@id and [page]=@page";
            int lastId = 0;
            Console.WriteLine("start " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    cmd.Parameters.AddWithValue("@id", lastId);
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                        }
                    }
                }
                foreach (var oInfo in lDoc)
                {
                    lastId = oInfo.id;
                    Console.Write("id " + oInfo.id + " " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "      \r");
                    oInfo.ResetBlankInfo();
                    using (var trans = Program.conn.BeginTransaction())
                    {
                        if (!System.IO.File.Exists(oInfo.target))
                        {
                            Console.WriteLine("no file " + oInfo.target);
                            continue;
                        }
                        using (PdfDocument document = new PdfDocument(new PdfReader(oInfo.target)))
                        {
                            var pageNumbers = document.GetNumberOfPages();
                            for (int page = 1; page <= pageNumbers; page++)
                            {
                                //new LocationTextExtractionStrategy creates a new text extraction renderer
                                LocationTextExtractionStrategy strategy = new LocationTextExtractionStrategy();
                                PdfCanvasProcessor parser = new PdfCanvasProcessor(strategy);
                                parser.ProcessPageContent(document.GetPage(page));
                                String s = strategy.GetResultantText();
                                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn, trans))
                                {
                                    cmd.Parameters.AddWithValue("@t", s);
                                    cmd.Parameters.AddWithValue("@id", oInfo.id);
                                    cmd.Parameters.AddWithValue("@page", page);
                                    var affected = cmd.ExecuteNonQuery();
                                    if (affected != 1) Console.WriteLine("id=" + oInfo.id + " page=" + page + " => " + affected + " row(s) affected");
                                }
                            }
                            //Console.WriteLine(pageText.ToString());
                        }
                        trans.Commit();
                    }
                }
                //return;
            }
        }

        static public void DoGetHeightWidthJP2IntoSQL()
        {
            var dicSizeJP2 = new Dictionary<String, CXY>();
            using (var fList = System.IO.File.OpenText(@"otherPath\JP2Sizes.txt"))
            {
                while (true)
                {
                    String l = fList.ReadLine();
                    if (l == null) break;
                    var t = l.Split('\t');
                    var oXY = new CXY();
                    int.TryParse(t[1], out oXY.X0);
                    int.TryParse(t[2], out oXY.X1);
                    int.TryParse(t[3], out oXY.Y0);
                    int.TryParse(t[4], out oXY.Y1);
                    dicSizeJP2.Add(t[0], oXY);
                }
            }
            Console.WriteLine("Read " + dicSizeJP2.Count + " lines");

            String sqlGet = @"select distinct top 100  " + CBinarNASList.FIELD_LIST + @"
                from BinarNASList bl
                where (1=1 or exists(select 1 from BinarPage bp where bp.id=bl.id and widthJP2 is null))
                and id > @lastid
                order by id";
            String sqlUpdate = @"update BinarPage set widthJP2=@w, heightJP2=@h
                where id=@id and page=@page";

            int lastid = 0;
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    cmd.Parameters.AddWithValue("@lastid", lastid);
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lastid = oInfo.id;
                            lDoc.Add(oInfo);
                        }
                    }
                }
                Console.Write("Bunch of " + lDoc.Count + " docs, lastid=" + lastid.ToString() + "               \r");
                foreach (var oInfo in lDoc)
                {
                    var oDirSource = new System.IO.DirectoryInfo(oInfo.folder);
                    if (!oDirSource.Exists)
                    {
                        Console.WriteLine("inexistant folder " + oInfo.folder);
                        continue;
                    }
                    var tabJP2Files = oDirSource.GetFiles(oInfo.basename + "_*.jp2");
                    if (tabJP2Files.Length != oInfo.nbPageJP2)
                    {
                        Console.WriteLine("bad JP2 number : " + tabJP2Files.Length.ToString() + " instead of " + oInfo.nbPageJP2.ToString());
                        continue;
                    }
                    if (oInfo.nbMultipage > 0)
                    {
                        // remove POM files
                        int nbRemovedJP2 = CWorker.RemovePOM(ref tabJP2Files);
                        Console.WriteLine("Removed " + nbRemovedJP2 + " PON from " + oInfo.folder);
                    }
                    tabJP2Files.OrderBy(Name => Name);

                    Console.Write(oInfo.subFolder + "\\" + oInfo.basename + "_*.jp2                  \r");
                    for (int iPage = 0; iPage < tabJP2Files.Length; iPage++)
                    {
                        String fpath = tabJP2Files[iPage].FullName;
                        CXY oXY;
                        var bOK = dicSizeJP2.TryGetValue(fpath, out oXY);
                        if (bOK)
                        {
                            //Console.WriteLine("success opening " + fpath + ", " + img.X0 + ", " + img.X1 + ", " + img.Y0 + ", " + img.Y1);
                            if (oXY.X0 != 0) Console.WriteLine("X0=" + oXY.X0.ToString() + " for " + fpath);
                            if (oXY.Y0 != 0) Console.WriteLine("Y0=" + oXY.Y0.ToString() + " for " + fpath);
                            using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn))
                            {
                                cmd.Parameters.AddWithValue("@w", oXY.X1 - oXY.X0);
                                cmd.Parameters.AddWithValue("@h", oXY.Y1 - oXY.Y0);
                                cmd.Parameters.AddWithValue("@id", oInfo.id);
                                cmd.Parameters.AddWithValue("@page", iPage + 1);
                                cmd.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            //Console.WriteLine("not found " + fpath);
                        }


                    }

                }
                //return;
            }
        }

        static public void DoGetHeightWidthIntoSQL()
        {
            String sqlGet = "select top 100 " + CBinarNASList.FIELD_LIST +
                " from BinarNASList where dateDone is not null" +
                " and [smallHeightPortrait] is null and [smallWidthPaysage] is null";
            String sqlUpdate = "update BinarNASList set [smallHeightPortrait]=@sh, [smallWidthPaysage]=@sw," +
                " [bigHeightPortrait]=@bh, [bigWidthPaysage]=@bw," +
                " [firstPagePaysage]=@fp,[nbPagePaysage] =@np" +
                " where id=@id";
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                        }
                    }
                }
                foreach (var oInfo in lDoc)
                {
                    //float smallest = 999999999.0F;
                    //float bigest = 0.0F;
                    using (PdfDocument document = new PdfDocument(new PdfReader(oInfo.target)))
                    {
                        var pageNumbers = document.GetNumberOfPages();
                        for (int page = 1; page <= pageNumbers; page++)
                        {
                            var oPage = document.GetPage(page);
                            var oRect = oPage.GetPageSizeWithRotation();
                            int w = (int)oRect.GetWidth();
                            int h = (int)oRect.GetHeight();
                            if (w < h)
                            {   // Portrait
                                if (oInfo.smallHeight == 0)
                                {
                                    oInfo.smallHeight = h;
                                    oInfo.bigHeight = h;
                                }
                                else
                                {
                                    if (oInfo.smallHeight > h) oInfo.smallHeight = h;
                                    if (oInfo.bigHeight < h) oInfo.bigHeight = h;
                                }
                            }
                            else
                            {   // Paysage
                                if (!oInfo.firstPagePaysage.HasValue) oInfo.firstPagePaysage = page;
                                oInfo.nbPagePaysage = (oInfo.nbPagePaysage ?? 0) + 1;
                                if (oInfo.smallWidth == 0)
                                {
                                    oInfo.smallWidth = w;
                                    oInfo.bigWidth = w;
                                }
                                else
                                {
                                    if (oInfo.smallWidth > w) oInfo.smallWidth = w;
                                    if (oInfo.bigWidth < w) oInfo.bigWidth = w;
                                }
                            }
                        }
                        //Console.WriteLine(pageText.ToString());

                    }
                    using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn))
                    {
                        cmd.Parameters.AddWithValue("@sh", ZeroIsDBNull(oInfo.smallHeight));
                        cmd.Parameters.AddWithValue("@sw", ZeroIsDBNull(oInfo.smallWidth));
                        cmd.Parameters.AddWithValue("@bh", ZeroIsDBNull(oInfo.bigHeight));
                        cmd.Parameters.AddWithValue("@bw", ZeroIsDBNull(oInfo.bigWidth));
                        cmd.Parameters.AddWithValue("@fp", NullIsDBNull(oInfo.firstPagePaysage));
                        cmd.Parameters.AddWithValue("@np", NullIsDBNull(oInfo.nbPagePaysage));
                        cmd.Parameters.AddWithValue("@id", oInfo.id);
                        cmd.ExecuteNonQuery();
                    }
                }
                //return;
            }
        }

        static Object ZeroIsDBNull(int x)
        {
            if (x == 0) return DBNull.Value;
            return x;
        }

        static Object ZeroIsDBNullLong(long x)
        {
            if (x == 0) return DBNull.Value;
            return x;
        }

        static Object NullIsDBNull(int? x)
        {
            if (!x.HasValue) return DBNull.Value;
            return x;
        }

        static public void DoGetFullTextIntoSQL()
        {
            String sqlGet = "select top 100 l." + CBinarNASList.FIELD_LIST +
                " from BinarNASList l left outer join BinarFullText t on l.id=t.id where dateDone is not null and t.id is null";
            String sqlUpdate = "insert into BinarFullText(id, textPDF) values (@id, @t)";
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                        }
                    }
                }
                foreach (var oInfo in lDoc)
                {
                    var pageText = new System.Text.StringBuilder();
                    pageText.Append(" ");
                    //read PDF using new PdfDocument and new PdfReader...
                    using (PdfDocument document = new PdfDocument(new PdfReader(oInfo.target)))
                    {
                        var pageNumbers = document.GetNumberOfPages();
                        for (int page = 1; page <= pageNumbers; page++)
                        {
                            //new LocationTextExtractionStrategy creates a new text extraction renderer
                            LocationTextExtractionStrategy strategy = new LocationTextExtractionStrategy();
                            PdfCanvasProcessor parser = new PdfCanvasProcessor(strategy);
                            parser.ProcessPageContent(document.GetPage(page));
                            pageText.Append(strategy.GetResultantText());
                        }
                        //Console.WriteLine(pageText.ToString());

                    }
                    pageText.Append(" ");
                    String t = System.Text.RegularExpressions.Regex.Replace(pageText.ToString(), @"\W+", " ").ToLower();
                    using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn))
                    {
                        cmd.Parameters.AddWithValue("@t", t);
                        cmd.Parameters.AddWithValue("@id", oInfo.id);
                        cmd.ExecuteNonQuery();
                    }
                }
                //return;
            }
        }

        static public void DoGetSizeBinarIntoSQL()
        {
            String sqlGet = "select top 100 " + CBinarNASList.FIELD_LIST +
                " from BinarNASList where dateDone is not null and SizePdfBinarized is null";
            String sqlUpdate = "update BinarNASList set SizePdfBinarized=@s where id=@id";
            for (; ; )
            {
                var lDoc = new List<CBinarNASList>();
                using (var cmd = new System.Data.SqlClient.SqlCommand(sqlGet, Program.conn))
                {
                    using (var rs = cmd.ExecuteReader())
                    {
                        if (!rs.HasRows) return;
                        while (rs.Read())
                        {
                            var oInfo = new CBinarNASList();
                            oInfo.LoadFromRS(rs);
                            lDoc.Add(oInfo);
                        }
                    }
                }
                foreach (var oInfo in lDoc)
                {
                    var oFile = new FileInfo(oInfo.target);
                    using (var cmd = new System.Data.SqlClient.SqlCommand(sqlUpdate, Program.conn))
                    {
                        cmd.Parameters.AddWithValue("@s", oFile.Length);
                        cmd.Parameters.AddWithValue("@id", oInfo.id);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
        }

        static public void DoTestOneDocAlreadyBinarized()
        {
            String dirOrig = @"somePath";
            String patternOrig = "some*.pdf";
            String pathTarget = @"someOther.pdf";

            String dirBinar = @"D:\work\temp\work2";

            InitICM();
            DoMergeOCR(dirOrig, patternOrig, pathTarget, dirBinar, 0, new List<int>());
        }

#if false
        static public void DoMergeOCR_mai_2023_from_TFS(String dirOrig, String patternOrig, String pathTarget, String dirBinar)
        {
            String patternBinar = "page*.pdf";

            var lOrig = (new DirectoryInfo(dirOrig)).GetFiles(patternOrig);
            var lBinar = (new DirectoryInfo(dirBinar)).GetFiles(patternBinar);
            if (lOrig.Length == 0)
            {
                Console.WriteLine("Error, no source PDFs");
                return;
            }
            if (lOrig.Length != lBinar.Length)
            {
                Console.WriteLine("Error, " + lOrig.Length + " source PDFs but " + lBinar.Length + " binarized PDFs");
                return;
            }
            //foreach (var o in lOrig) Console.WriteLine("before sort orig " + o.Name);
            //foreach (var o in lBinar) Console.WriteLine("before sort binar " + o.Name);
            lOrig.OrderBy(Name => Name);
            lBinar.OrderBy(Name => Name);
            //foreach (var o in lOrig) Console.WriteLine("after sort orig " + o.Name);
            //foreach (var o in lBinar) Console.WriteLine("after sort binar " + o.Name);

            using (var readerOrig = new iText.Kernel.Pdf.PdfReader(lOrig[0].FullName))
            using (var writerTarget = new iText.Kernel.Pdf.PdfWriter(pathTarget))
            using (var docTarget = new iText.Kernel.Pdf.PdfDocument(readerOrig, writerTarget))
            {
                var merger = new PdfMerger(docTarget);
                merger.SetCloseSourceDocuments(true);
                for (int iPage = 0; iPage < lOrig.Length; iPage++)
                {
                    if (iPage > 0)
                    {
                        using (var readerOrig2 = new iText.Kernel.Pdf.PdfReader(lOrig[iPage].FullName))
                        {
                            using (var docOrig2 = new PdfDocument(readerOrig2))
                            {
                                //var merger = new PdfMerger(docTarget);
                                //merger.SetCloseSourceDocuments(true);
                                merger.Merge(docOrig2, 1, 1);
                                //merger.Close();
                            }
                        }
                    }
                    //Console.WriteLine("after merge, nbpage=" + docTarget.GetNumberOfPages());
                    var editor = new PdfCanvasRemoveImages();
                    editor.EditPage(docTarget, iPage + 1);

                    var pageTarget = docTarget.GetPage(iPage + 1);
                    PdfCanvas canvas = new PdfCanvas(pageTarget.NewContentStreamBefore(),
                            docTarget.GetPage(iPage + 1).GetResources(), docTarget);
                    using (var readerBinar = new iText.Kernel.Pdf.PdfReader(lBinar[iPage].FullName))
                    using (var binarDoc = new PdfDocument(readerBinar))
                    {
                        PdfFormXObject imageBinar = binarDoc.GetFirstPage().CopyAsFormXObject(docTarget);
                        //canvas.AddXObjectAt(page, 0, 0);
                        var targetRect = pageTarget.GetPageSize();
                        canvas.AddXObjectFittedIntoRectangle(imageBinar, targetRect);
                    }
                }
                merger.Close();
                docTarget.Close();
            }
        }
#endif

        static public void DoMergeOCR(String dirOrig, String patternOrig, String pathTarget, String dirBinar, int id, List<int> lBlankPages)
        {
            String patternBinar = "page*.pdf";
            var lOrig = (new DirectoryInfo(dirOrig)).GetFiles(patternOrig);
            CWorker.RemovePOM(ref lOrig);
            var lBinar = (new DirectoryInfo(dirBinar)).GetFiles(patternBinar);
            if (lOrig.Length == 0)
            {
                if (bTest) Console.WriteLine("Error, no source PDFs");
                else LogErrorSQL("MergeOCR", "no source PDF", dirOrig, null, 0, null, 0, id);
                return;
            }
            if (lOrig.Length != lBinar.Length)
            {
                if (bTest) Console.WriteLine("Error, " + lOrig.Length + " source PDFs but " + lBinar.Length + " binarized PDFs");
                else LogErrorSQL("MergeOCR", lOrig.Length + " source PDFs but " + lBinar.Length + " binarized PDFs", dirOrig, null, 0, null, 0, id);
                return;
            }
            lOrig.OrderBy(Name => Name);
            lBinar.OrderBy(Name => Name);

#if true   // PDF/A
            using (var readICM = new MemoryStream(icm, false))
            using (var writerTarget = new iText.Kernel.Pdf.PdfWriter(pathTarget))
            using (var docTarget = new iText.Pdfa.PdfADocument(writerTarget, PdfAConformanceLevel.PDF_A_2B,
                new PdfOutputIntent("Custom", "", "http://www.color.org", "sRGB IEC61966-2.1", readICM)))
#else
            using (var readerOrig = new iText.Kernel.Pdf.PdfReader(lOrig[0].FullName))
            using (var writerTarget = new iText.Kernel.Pdf.PdfWriter(pathTarget))
            using (var docTarget = new iText.Kernel.Pdf.PdfDocument(readerOrig, writerTarget))
#endif
            {


                for (int iPage = 0; iPage < lOrig.Length; iPage++)
                {
                    if (lBlankPages.Contains(iPage + 1)) continue;

                    // create page
                    PdfPage oNewPage;
                    using (var readerOrig = new iText.Kernel.Pdf.PdfReader(lOrig[iPage].FullName))
                    using (var docIn = new iText.Kernel.Pdf.PdfDocument(readerOrig))
                    {
                        var oPageOrig = docIn.GetPage(1);
                        var sizeOrig = oPageOrig.GetPageSize();
                        float w = sizeOrig.GetWidth();
                        float h = sizeOrig.GetHeight();
                        float percentage = Math.Min(w / SIZE_LIMIT, h / SIZE_LIMIT);
                        if (percentage > 1.0f)
                        {
                            if (h > w) percentage = NEW_SIZE / h;
                            else percentage = NEW_SIZE / w;
                            w *= percentage;
                            h *= percentage;
                        }
                        else
                        {
                            percentage = 1.0f;
                        }
                        var newSize = new iText.Kernel.Geom.PageSize(w, h);
                        oNewPage = docTarget.AddNewPage(newSize);

                        // Copy font & ExtGState
                        var oldResources = oPageOrig.GetResources();
                        var oldDicFonts = oldResources.GetResource(PdfName.Font);
                        var newResources = new PdfResources();
                        var lResourceName = new List<String>();
                        if (oldDicFonts != null)
                            foreach (var kvFont in ((PdfDictionary)oldDicFonts.CopyTo(docTarget)).EntrySet())
                            {
                                var font = PdfFontFactory.CreateFont((PdfDictionary)kvFont.Value);
                                var newName = newResources.AddFont(docTarget, font);
                                lResourceName.Add(kvFont.Key.ToString());
                                if (newName != kvFont.Key)
                                {
                                    // keep same name
                                    var newValue = newResources.GetResource(PdfName.Font);
                                    newValue.Put(kvFont.Key, newValue.Get(newName));
                                    newValue.Remove(newName);
                                }
                            }
                        var oldDicExtGState = oldResources.GetResource(PdfName.ExtGState);
                        if (oldDicExtGState == null)
                        {
                            Program.LogErrorSQL("PageResize", "no ExtGState in page " + (iPage + 1), null, null, 0, null, 0, id);
                        }
                        else
                        {
                            foreach (var kvExGState in ((PdfDictionary)oldDicExtGState.CopyTo(docTarget)).EntrySet())
                            {
                                var newName = newResources.AddExtGState((PdfDictionary)kvExGState.Value);
                                lResourceName.Add(kvExGState.Key.ToString());
                                if (newName != kvExGState.Key)
                                {
                                    // keep same name
                                    var newValue = newResources.GetResource(PdfName.ExtGState);
                                    newValue.Put(kvExGState.Key, newValue.Get(newName));
                                    newValue.Remove(newName);
                                }
                            }
                        }
                        oNewPage.SetResources(newResources);

                        // Copy Content

                        var nbOldContent = oPageOrig.GetContentStreamCount();
                        for (var iContent = 0; iContent < nbOldContent; iContent++)
                        {
                            var oStreamContent = oPageOrig.GetContentStream(iContent);
                            var bContent = oStreamContent.GetBytes();
                            if (DEBUG_CMAP)
                            {
                                String FileNameTrace = @"d:\tmp\content" + iContent + "." + id.ToString("000000") + ".page" + iPage + ".txt";
                                System.IO.File.WriteAllBytes(FileNameTrace, bContent);
                            }
                            // WARNING sContent is only used for searching PdfNames in the stream, it is spoilt for the actual text of the page (encoding issue)
                            var sContent = System.Text.Encoding.ASCII.GetString(oStreamContent.GetBytes());

                            //Console.WriteLine("page " + iPage + ", iContent=" + iContent + "/" + nbOldContent + ", content=" + sContent);
                            foreach (var sName in lResourceName)
                            {
                                if (sContent.Contains(sName))
                                {
                                    PdfStream newStream;
                                    // keep it
                                    if (iContent == 0)
                                    {
                                        // replace existing (empty) content
                                        var oContent = oNewPage.GetPdfObject().Get(PdfName.Contents);
                                        if (oContent.GetObjectType() == PdfObject.STREAM)
                                        {
                                            newStream = (PdfStream)oContent;
                                        }
                                        else
                                        {
                                            // warning !
                                            Program.LogErrorSQL("PageResize", "Initial type of content is " + oContent.GetObjectType() + " page " + (iPage + 1), null, null, 0, null, 0, id);
                                            newStream = oNewPage.NewContentStreamAfter();
                                        }
                                    }
                                    else
                                    {
                                        // add content
                                        newStream = oNewPage.NewContentStreamAfter();
                                    }
                                    //dirContents = (PdfArray)oNewPage.GetPdfObject().Get(PdfName.Contents);
                                    var sPercentage = (percentage / 10.0f).ToString(System.Globalization.CultureInfo.InvariantCulture);
                                    //int idxMatrix = sContent.IndexOf("0.1 0 0 0.1 0 0");
                                    // serach Matrix in Content
                                    int idxMatrix = -1;
                                    int contentLength = bContent.Length;
                                    for (int i = 0; i <= contentLength - _patternMatrixLength; i++)
                                    {
                                        if (bContent[i] == _patternMatrixBytes[0])
                                        {
                                            for (int m = 1; m < contentLength; m++)
                                            {
                                                if (bContent[i + m] != _patternMatrixBytes[m]) break;
                                                if (m == _patternMatrixLength - 1)
                                                {
                                                    idxMatrix = i;
                                                    break;
                                                }
                                            }
                                            if (idxMatrix >= 0) break;
                                        }
                                    }
                                    byte[] bContentResized;
                                    if (idxMatrix < 0)
                                    {
                                        Program.LogErrorSQL("PageResize", "Missing \"0.1 0 0 0.1 0 0\" page " + (iPage + 1), null, null, 0, null, 0, id);
                                        bContentResized = bContent;
                                    }
                                    else
                                    {
                                        if (idxMatrix > 3)
                                            Program.LogErrorSQL("PageResize", "index of \"0.1 0 0 0.1 0 0\" is " + idxMatrix + " page " + (iPage + 1), null, null, 0, null, 0, id);
                                        //bContentResized = sContent.Remove(idxMatrix) + sPercentage + " 0 0 " + sPercentage + " 0 0" + sContent.Substring(idxMatrix + 15);
                                        // Replace Matrix in Content
                                        var sReplace = sPercentage + " 0 0 " + sPercentage + " 0 0";
                                        var bReplace = System.Text.Encoding.ASCII.GetBytes(sReplace);
                                        int lReplace = bReplace.Length;
                                        bContentResized = new byte[contentLength - _patternMatrixLength + lReplace];
                                        Buffer.BlockCopy(bContent, 0, bContentResized, 0, idxMatrix);
                                        Buffer.BlockCopy(bReplace, 0, bContentResized, idxMatrix, lReplace);
                                        Buffer.BlockCopy(bContent, idxMatrix + _patternMatrixLength, bContentResized, idxMatrix + lReplace, contentLength - (idxMatrix + _patternMatrixLength));
                                    }
                                    newStream.SetData(bContentResized);

                                    break;
                                }
                            }
                        }

                        // Copy image

                        // get name of image in orig
                        var oldDicImg = oldResources.GetResource(PdfName.XObject);
                        PdfName oldNameImage = null;
                        if (oldDicExtGState == null)
                        {
                            Program.LogErrorSQL("PageResize", "no XObject in page " + (iPage + 1), null, null, 0, null, 0, id);
                        }
                        else
                        {
                            foreach (var kvImage in oldDicImg.EntrySet())
                            {
                                oldNameImage = kvImage.Key;
                                break;  // should only have one
                            }
                        }

                        using (var readerBinar = new iText.Kernel.Pdf.PdfReader(lBinar[iPage].FullName))
                        using (var binarDoc = new PdfDocument(readerBinar))
                        {
                            var binarDicImg = binarDoc.GetPage(1).GetResources().GetResource(PdfName.XObject);

                            foreach (var kvImage in binarDicImg.EntrySet())
                            {
                                if (kvImage.Value.GetObjectType() != PdfObject.STREAM)
                                {
                                    Console.WriteLine("id=" + id + ", page=" + iPage + ", XObject not stream");
                                    continue;
                                }
                                var newName = newResources.AddForm((PdfStream)kvImage.Value.CopyTo(docTarget));
                                if (oldNameImage == null)
                                {
                                    Console.WriteLine("id=" + id + ", page=" + iPage + ", No image in orig");
                                    break;
                                }
                                if (newName != oldNameImage)
                                {
                                    var newDicImages = newResources.GetResource(PdfName.XObject);
                                    newDicImages.Put(oldNameImage, newDicImages.Get(newName));
                                    newDicImages.Remove(newName);
                                }
                                break;  // Should only have one
                            }
                        }
                    }
                }
                String sError = null;
                sError = Program.DoMergeFontsDoc(docTarget, id);
                if (sError != null)
                {
                    //Console.WriteLine("id=" + id + " " + sError);
                    Program.LogErrorSQL("MergeFont", sError, null, null, 0, null, 0, id);
                }
                var docInfo = docTarget.GetDocumentInfo();
                docInfo.SetCreator("MergeScannedPages");
                docTarget.Close();
            }
        }


        static void DoScanNASIntoSQL()
        {
            System.IO.DirectoryInfo oDir = new System.IO.DirectoryInfo(INPUT_FOLDER);
            foreach (System.IO.DirectoryInfo oDir2 in oDir.GetDirectories())
            {
                string[] tabPart = oDir2.Name.Split('_');
                if (tabPart.Length != 3 || tabPart[0] != "BOX" || tabPart[1] != "GC")
                {
                    LogErrorSQL("bad folder level 1", oDir2.Name, oDir2.FullName, null, 0, null, 0, 0);
                    continue;
                }
                String Box = tabPart[2];
                foreach (System.IO.DirectoryInfo oDir3 in oDir2.GetDirectories())
                {
                    if (oDir3.Name == "Targets_BOX_GC_" + Box) continue;
                    //Console.WriteLine(oDir.FullName + "\\" + oDir2.Name);
                    DoScanNASDir(oDir3, oDir2, tabPart[2]);
                }
                DoScanNASDir(oDir2, null, Box);
            }
        }

        static void DoScanNASDir(System.IO.DirectoryInfo oDir, System.IO.DirectoryInfo oDirParent, String Box)
        {
            int Seqno = 0;
            int Joker = 0;
            String Lang = null;
            if (oDirParent != null)
            {
                string[] tabPart = oDir.Name.Split('_');
                if (tabPart.Length != 2)
                {
                    LogErrorSQL("bad folder level 2", oDir.Name, oDir.FullName, Box, Seqno, Lang, Joker, 0);
                    return;
                }
                if ((!int.TryParse(tabPart[0], out Seqno)) || Seqno == 0)
                {
                    LogErrorSQL("bad Seqno", oDir.Name, oDir.FullName, Box, Seqno, Lang, Joker, 0);
                    return;
                }
                Lang = tabPart[1];
                if (!IsLangValid(ref Lang))
                {
                    LogErrorSQL("bad Lang", oDir.Name, oDir.FullName, Box, Seqno, Lang, Joker, 0);
                    return;
                }
                foreach (System.IO.DirectoryInfo oDirX in oDir.GetDirectories())
                {
                    LogErrorSQL("Unexpected folder", oDirX.Name, oDirX.FullName, Box, Seqno, Lang, Joker, 0);
                }
            }
            Console.Write(oDir.FullName + "       \r");

            // list of document (same ID, same language)
            var dicDoc = new System.Collections.Generic.Dictionary<String, CDocSize>();
            foreach (System.IO.FileInfo oFile in oDir.GetFiles())
            {
                bool bPDF = false;
                bool bMultipage = false;
                switch (oFile.Extension.ToLower())
                {
                    case ".pdf":
                        bPDF = true;
                        break;
                    case ".jp2":
                        if (oDirParent == null && oFile.Name.Substring(0, 2) == "OZ") continue;   // Ignore
                        break;
                    case ".txt":                        // ignore
                        continue;
                    case ".bridgelabelsandratings":
                        continue;
                    case ".ds_store":
                        continue;
                    case ".csv":
                        if (oFile.Name.EndsWith("_Checksum.csv")) continue;
                        break;
                    default:
                        LogErrorSQL("bad filename", "bad extension in " + oFile.Name, oFile.FullName, Box, Seqno, Lang, Joker, 0);
                        continue;
                }
                string[] tabPart = System.IO.Path.GetFileNameWithoutExtension(oFile.Name).Split('_');

                if (tabPart.Length == 1)
                {
                    LogErrorSQL("bad filename", "Only one part in" + oFile.Name + ", ignore", oFile.FullName, Box, Seqno, Lang, Joker, 0);
                    continue;
                }
                String thisLang = null;
                int normalNbPart;
                String basename;
                if (IsLangValid(ref tabPart[1]))
                {
                    thisLang = tabPart[1];
                    normalNbPart = 3;
                    basename = tabPart[0] + "_" + tabPart[1];
                }
                else
                {
                    if (!int.TryParse(tabPart[1], out int foo))
                    {
                        LogErrorSQL("bad filename", "Invalid Lang in filename in " + oFile.Name + ", ignore", oFile.FullName, Box, Seqno, Lang, Joker, 0);
                        continue;
                    }
                    // keep thisLang Empty
                    normalNbPart = 2;
                    basename = tabPart[0];
                }
                if (oDirParent != null && thisLang != Lang)
                {
                    if (string.IsNullOrWhiteSpace(thisLang))
                    {
                        LogErrorSQL("bad filename", "not language (keep " + Lang + ") in " + oFile.Name, oFile.FullName, Box, Seqno, Lang, Joker, 0);
                    }
                    else
                    {
                        LogErrorSQL("bad filename", "not same language in " + oFile.Name, oFile.FullName, Box, Seqno, Lang, Joker, 0);
                        continue;
                    }
                }

                if (tabPart.Length != normalNbPart)
                {
                    String lastPart = tabPart[tabPart.Length - 1];
                    switch (lastPart)
                    {
                        case "COM":
                            bMultipage = true;
                            break;
                        case "POM":
                            if (tabPart.Length != normalNbPart + 1)
                            {
                                LogErrorSQL("bad filename", "not " + (normalNbPart + 1) + " parts for POM in " + oFile.Name + ", ignore", oFile.FullName, Box, Seqno, Lang, Joker, 0);
                                continue;
                            }
                            break;
                        default:
                            LogErrorSQL("bad filename", "not " + normalNbPart + " parts in " + oFile.Name + ", ignore", oFile.FullName, Box, Seqno, Lang, Joker, 0);
                            continue;
                    }
                }
                bool bForce = false;
                if (!int.TryParse(tabPart[0], out int thisNo))
                {
                    // fix error in data
                    if (oDirParent == null && Box == "000385" && tabPart[0] == "81-")
                    {
                        thisNo = 212934;
                        Seqno = 212934;
                        Joker = 0;
                        Lang = "SPA";
                        bForce = true;
                    }
                    else
                    {
                        LogErrorSQL("bad filename", "part1 not numeric in " + oFile.Name, oFile.FullName, Box, Seqno, Lang, Joker, 0);
                        continue;
                    }
                }
                if (bForce)
                {
                    // don't touch
                }
                else if (oDirParent == null)
                {
                    Joker = thisNo;
                }
                else
                {
                    if (thisNo != Seqno)
                    {
                        LogErrorSQL("bad filename", "not the same Seqno in " + oFile.Name, oFile.FullName, Box, Seqno, Lang, Joker, 0);
                        continue;
                    }
                }
                String dickey = thisNo + "_" + Lang;
                //Boolean bBig = oFile.Length > 8500000;
                //if (oFile.Length > maxSize) maxSize = oFile.Length;
                if (dicDoc.TryGetValue(dickey, out CDocSize oDocSize))
                {
                    if (oDocSize.Basename != basename)
                    {
                        LogErrorSQL("different basename", "Other basename=" + oDocSize.Basename, oFile.FullName, Box, Seqno, Lang, Joker, 0);
                        continue;
                    }
                }
                else
                {
                    oDocSize = new CDocSize(basename, Joker, Seqno, Lang);
                    dicDoc.Add(dickey, oDocSize);
                }
                if (bPDF) oDocSize.AddPDF(oFile.Length);
                else oDocSize.AddJP2(oFile.Length);
                if (bMultipage) oDocSize.nbMultipage++;
            }
            String sql = "insert into BinarNASList(Path, Box, Seqno, Lang, Joker, Basename, nbPagePdfOrig, SizePdfOrig," +
                " nbPageJP2, SizeJP2, MaxSizeJP2, NbMultipage, DoubleNumber)"
                + " values(@p, @b, @s, @l, @j, @bn, @np, @sp, @nj, @sj, @mj, @nm" +
                ", case when exists(select * from BinarNASList b2" +
                " where isNull(b2.Lang, '')=isNull(@l, '')" +
                " and isNull(b2.Seqno, 0)=isNull(@s, 0)" +
                " and isNull(b2.Joker, 0)=isNull(@j, 0)) then 1 else null end)";
            foreach (System.Collections.Generic.KeyValuePair<String, CDocSize> kv in dicDoc)
            {
                using (var cmd = new System.Data.SqlClient.SqlCommand(sql, conn))
                {
                    cmd.Parameters.AddWithValue("@p", oDir.FullName);
                    cmd.Parameters.AddWithValue("@b", Box);
                    cmd.Parameters.AddWithValue("@s", (kv.Value.Seqno == 0) ? DBNull.Value : ((Object)kv.Value.Seqno));
                    cmd.Parameters.AddWithValue("@l", String.IsNullOrEmpty(kv.Value.Lang) ? DBNull.Value : (Object)kv.Value.Lang);
                    cmd.Parameters.AddWithValue("@j", (kv.Value.Joker == 0) ? DBNull.Value : ((Object)kv.Value.Joker));
                    cmd.Parameters.AddWithValue("@bn", (kv.Value.Basename is null) ? DBNull.Value : ((Object)kv.Value.Basename));
                    cmd.Parameters.AddWithValue("@np", kv.Value.nbPagePDF);
                    cmd.Parameters.AddWithValue("@sp", kv.Value.totalSizePDF);
                    cmd.Parameters.AddWithValue("@nj", kv.Value.nbPageJP2);
                    cmd.Parameters.AddWithValue("@sj", kv.Value.totalSizeJP2);
                    cmd.Parameters.AddWithValue("@mj", kv.Value.maxSizeJP2);
                    cmd.Parameters.AddWithValue("@nm", (kv.Value.nbMultipage == 0) ? DBNull.Value : (Object)kv.Value.nbMultipage);
                    try
                    {
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        LogErrorSQL("Erreur SQL", e.Message, oDir.FullName, Box, Seqno, Lang, Joker, 0);
                        continue;
                    }
                }
            }
        }

        static bool IsLangValid(ref String Lang)
        {
            switch (Lang)
            {
                case "ENG": return true;
                case "FRE": return true;
                case "SPA": return true;
                case "MUL": return true;
                case "PLU": return true;
                case "SP":
                    Lang = "SPA";
                    return true;
                case "ENG44":
                    Lang = "ENG";
                    return true;
            }
            return false;
        }

        public static void LogErrorSQL(String Key, String Text, String Path, String Box, int Seqno, String Lang, int Joker, int iddoc)
        {
            Console.WriteLine(Key + " " + Text + " " + Path + "   ");
            if (Program.bTest) return;
            String sql = "insert into BinarError(DateLog, Mode, Text, Path, Box, Seqno, Lang, Joker, IDDoc)" +
                " values(GETDATE(), @k, @t, @p, @b, @s, @l, @j, @i)";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("@k", Key);
                cmd.Parameters.AddWithValue("@t", Text);
                cmd.Parameters.AddWithValue("@p", ((object)Path) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@b", ((object)Box) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@s", (Seqno == 0) ? DBNull.Value : (object)Seqno);
                cmd.Parameters.AddWithValue("@l", ((object)Lang) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@j", (Joker == 0) ? DBNull.Value : (object)Joker);
                cmd.Parameters.AddWithValue("@i", (iddoc == 0) ? DBNull.Value : (object)iddoc);
                cmd.ExecuteNonQuery();
            }
        }

        public static void Log(String Key, String Text, String Path, String Box, int? Seqno, String Lang, int? Joker, int? Worker, bool bConsole = true, int? idDoc = null)
        {
            if (bConsole) Console.WriteLine(Key + " " + (Text ?? Path) + "   ");
            String sql = "insert into BinarLog(DateLog, Mode, Text, Path, Box, Seqno, Lang, Joker, Worker, IDDoc)" +
                " values(GETDATE(), @k, @t, @p, @b, @s, @l, @j, @w, @id)";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("@k", Key);
                cmd.Parameters.AddWithValue("@t", ((object)Text) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@p", ((object)Path) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@b", ((object)Box) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@s", ((object)Seqno) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@l", ((object)Lang) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@j", ((object)Joker) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@w", ((object)Worker) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@id", ((object)idDoc) ?? DBNull.Value);
                cmd.ExecuteNonQuery();
            }
        }

        public static String LogBinds(System.Data.SqlClient.SqlParameterCollection parameters)
        {
            var oRet = new System.Text.StringBuilder();
            String Separ = "[";
            foreach (System.Data.SqlClient.SqlParameter p in parameters)
            {
                oRet.Append(Separ + p.ParameterName + "=" + (p.Value ?? ((Object)"<NULL>")));
                Separ = ", ";
            }
            if (Separ != "[") oRet.Append("]");
            return oRet.ToString();
        }
        static void DoConvert()
        {
            Program.InitICM();
            System.Collections.Generic.List<System.Threading.WaitHandle> lh = new System.Collections.Generic.List<System.Threading.WaitHandle>();

            if (!bTest)
            {
                Program.Log("Start", "Convert", null, null, 0, null, 0, 0);
            }

            for (int iWorker = 0; iWorker < NB_PROCESS; iWorker++)
            {
                tabWorker[iWorker] = new CWorker(iWorker + 1);
            }

            for (; ; )
            {
                lh.Clear();
                tabWorker[0].TryNext();
                if (tabWorker[0].status != CWorker.EStatus.ended) lh.Add(tabWorker[0]);
                for (int iWorker = 1; iWorker < NB_PROCESS; iWorker++)
                {
                    tabWorker[iWorker].TryNext();
                    if (tabWorker[iWorker].status != CWorker.EStatus.ended) lh.Add(tabWorker[iWorker]);
                }
                if (lh.Count == 0) break; ;
                if (Program.B_TEST_FOLDER) continue;

                int iWait = System.Threading.WaitHandle.WaitAny(lh.ToArray(), 5000);
                if (iWait == System.Threading.WaitHandle.WaitTimeout)
                {
                    //Console.Write("Timeout waiting " + lh.Count.ToString() + " processes                       \r");
                    //Console.Write("Timeout waiting " + lh.Count.ToString() + " processes                       \r");
                }
            }
            if (!bTest)
            {
                Program.Log("End", "Convert", null, null, 0, null, 0, 0);
            }
        }
    }

    class CDocSize
    {
        public string Basename;
        public int Joker;
        public int Seqno;
        public String Lang;
        public long maxSizeJP2;
        public long totalSizeJP2;
        public long totalSizePDF;
        public int nbPageJP2;
        public int nbPagePDF;
        public int nbMultipage;

        public CDocSize(String paramBasename, int paramJocker, int paramSeqno, String paramLang)
        {
            Basename = paramBasename;
            Joker = paramJocker;
            Seqno = paramSeqno;
            Lang = paramLang;
        }

        public void AddJP2(long l)
        {
            if (l > maxSizeJP2) maxSizeJP2 = l;
            totalSizeJP2 += l;
            nbPageJP2++;
        }

        public void AddPDF(long l)
        {
            totalSizePDF += l;
            nbPagePDF++;
        }
    }

    class CBinarNASList
    {
        public int id;
        public String path;
        public String box;
        public int? Seqno;
        public String Lang;
        public int? Joker;
        public String basename;
        public int nbPagePdfOrig;
        public long sizePdfOrig;
        public int nbPageJP2;
        public long sizeJP2;
        public long maxSizeJP2;
        public long sizePdfBinarized;
        public DateTime? dateDone;
        public String lastError;
        public int nbMultipage;
        public bool doubleNumber = false;
        public bool useModifedOrigin = false;
        public string Ignore;
        public bool ExistsInUNESDOC = false;
        public int smallHeight;
        public int smallWidth;
        public int bigHeight;
        public int bigWidth;
        public DateTime? DateResize;
        public int? firstPagePaysage;
        public int? nbPagePaysage;
        public int? nbBlankPage;
        public long sizeSmallestBlankPage;
        public long sizeBigestBlankPage;
        public int pageSmallestBlank;
        public int pageBigestBlank;
        public int firstPage5Char;
        public int lastPage5Char;
        public int nbPage5Char;
        public bool manual = false;

        public String folder;
        public String subFolder;
        public String outDir;
        public String target;

        public enum EFields : int
        {
            id = 0,
            Path,
            Box,
            Seqno,
            Lang,
            Joker,
            Basename,
            nbPagePdfOrig,
            SizePdfOrig,
            nbPageJP2,
            SizeJP2,
            MaxSizeJP2,
            SizePdfBinarized,
            DateDone,
            LastError,
            NbMultipage,
            DoubleNumber,
            UseModifiedOrig,
            Ignore,
            ExistsInUNESDOC,
            smallHeight,
            smallWidth,
            bigHeight,
            bigWidth,
            DateResize,
            firstPagePaysage,
            nbPagePaysage,
            nbBlankPage,
            sizeSmallestBlankPage,
            sizeBigestBlankPage,
            pageSmallestBlank,
            pageBigestBlank,
            firstPage5Char,
            lastPage5Char,
            nbPage5Char,
            manual,
        }

        public const String FIELD_LIST = "id, Path, Box, Seqno, Lang, Joker, Basename" +
            ", nbPagePdfOrig, SizePdfOrig, nbPageJP2, SizeJP2, MaxSizeJP2" +
            ", SizePdfBinarized, DateDone, LastError, NbMultipage, DoubleNumber" +
            ", UseModifiedOrig, Ignore, ExistsInUNESDOC, smallHeightPortrait, smallWidthPaysage" +
            ", bigHeightPortrait, bigWidthPaysage, DateResize, firstPagePaysage, nbPagePaysage" +
            ", nbBlankPage, sizeSmallestBlankPage, sizeBigestBlankPage, pageSmallestBlank, pageBigestBlank" +
            ", firstPage5Char, lastPage5Char, nbPage5Char, manual";

        public void LoadFromRS(System.Data.SqlClient.SqlDataReader rs)
        {
            id = rs.GetInt32((int)EFields.id);
            path = rs.GetString((int)EFields.Path);
            box = rs.GetString((int)EFields.Box);
            if (!rs.IsDBNull((int)EFields.Seqno)) Seqno = rs.GetInt32((int)EFields.Seqno);
            if (!rs.IsDBNull((int)EFields.Lang)) Lang = rs.GetString((int)EFields.Lang);
            if (!rs.IsDBNull((int)EFields.Joker)) Joker = rs.GetInt32((int)EFields.Joker);
            basename = rs.GetString((int)EFields.Basename);
            nbPagePdfOrig = rs.GetInt32((int)EFields.nbPagePdfOrig);
            sizePdfOrig = rs.GetInt64((int)EFields.SizePdfOrig);
            nbPageJP2 = rs.GetInt32((int)EFields.nbPageJP2);
            sizeJP2 = rs.GetInt64((int)EFields.SizeJP2);
            maxSizeJP2 = rs.GetInt64((int)EFields.MaxSizeJP2);
            if (!rs.IsDBNull((int)EFields.SizePdfBinarized)) sizePdfBinarized = rs.GetInt64((int)EFields.SizePdfBinarized);
            if (!rs.IsDBNull((int)EFields.DateDone)) dateDone = rs.GetDateTime((int)EFields.DateDone);
            if (!rs.IsDBNull((int)EFields.LastError)) lastError = rs.GetString((int)EFields.LastError);
            if (!rs.IsDBNull((int)EFields.NbMultipage)) nbMultipage = rs.GetInt32((int)EFields.NbMultipage);
            if (!rs.IsDBNull((int)EFields.DoubleNumber)) doubleNumber = rs.GetBoolean((int)EFields.DoubleNumber);
            if (!rs.IsDBNull((int)EFields.UseModifiedOrig)) useModifedOrigin = rs.GetBoolean((int)EFields.UseModifiedOrig);
            if (!rs.IsDBNull((int)EFields.Ignore)) Ignore = rs.GetString((int)EFields.Ignore);
            if (!rs.IsDBNull((int)EFields.ExistsInUNESDOC)) ExistsInUNESDOC = rs.GetBoolean((int)EFields.ExistsInUNESDOC);
            if (!rs.IsDBNull((int)EFields.smallHeight)) smallHeight = rs.GetInt32((int)EFields.smallHeight);
            if (!rs.IsDBNull((int)EFields.smallWidth)) smallWidth = rs.GetInt32((int)EFields.smallWidth);
            if (!rs.IsDBNull((int)EFields.bigHeight)) bigHeight = rs.GetInt32((int)EFields.bigHeight);
            if (!rs.IsDBNull((int)EFields.bigWidth)) bigWidth = rs.GetInt32((int)EFields.bigWidth);
            if (!rs.IsDBNull((int)EFields.DateResize)) DateResize = rs.GetDateTime((int)EFields.DateResize);
            if (!rs.IsDBNull((int)EFields.firstPagePaysage)) firstPagePaysage = rs.GetInt32((int)EFields.firstPagePaysage);
            if (!rs.IsDBNull((int)EFields.nbPagePaysage)) bigWidth = rs.GetInt32((int)EFields.nbPagePaysage);
            if (!rs.IsDBNull((int)EFields.nbBlankPage)) nbBlankPage = rs.GetInt32((int)EFields.nbBlankPage);
            if (!rs.IsDBNull((int)EFields.sizeSmallestBlankPage)) sizeSmallestBlankPage = rs.GetInt64((int)EFields.sizeSmallestBlankPage);
            if (!rs.IsDBNull((int)EFields.sizeBigestBlankPage)) sizeBigestBlankPage = rs.GetInt64((int)EFields.sizeBigestBlankPage);
            if (!rs.IsDBNull((int)EFields.pageSmallestBlank)) pageSmallestBlank = rs.GetInt32((int)EFields.pageSmallestBlank);
            if (!rs.IsDBNull((int)EFields.pageBigestBlank)) pageBigestBlank = rs.GetInt32((int)EFields.pageBigestBlank);
            if (!rs.IsDBNull((int)EFields.firstPage5Char)) firstPage5Char = rs.GetInt32((int)EFields.firstPage5Char);
            if (!rs.IsDBNull((int)EFields.lastPage5Char)) lastPage5Char = rs.GetInt32((int)EFields.lastPage5Char);
            if (!rs.IsDBNull((int)EFields.nbPage5Char)) nbPage5Char = rs.GetInt32((int)EFields.nbPage5Char);
            if (!rs.IsDBNull((int)EFields.manual)) manual = rs.GetBoolean((int)EFields.manual);

            if (useModifedOrigin)
            {
                Console.WriteLine("Using modified for " + path + " " + basename + "     ");
                folder = path.Replace(Program.INPUT_FOLDER, Program.INPUT_MODIFIED);
            }
            else
            {
                folder = path;
            }
            subFolder = path.Replace(Program.INPUT_FOLDER, "");
            if (Program.bTest)
                outDir = Program.OUTPUT_FOLDER_TEST + subFolder;
            else
                outDir = Program.OUTPUT_FOLDER_PROD + subFolder;
            target = outDir + "\\" + basename + ".pdf";
        }

        public void MarkLastError(String sError)
        {
            String sql = "update BinarNASList set LastError = @e where id = @id";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                cmd.Parameters.AddWithValue("@e", sError);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.ExecuteNonQuery();
            }
        }

        public void MarkDone()
        {
            String sql = "update BinarNASList set DateDone = GETDATE() where id = @id";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                cmd.Parameters.AddWithValue("@id", id);
                cmd.ExecuteNonQuery();
            }
        }

        public void ResetBlankInfo()
        {
            nbBlankPage = null;
            sizeSmallestBlankPage = 0;
            sizeBigestBlankPage = 0;
            pageSmallestBlank = 0;
            pageBigestBlank = 0;
            firstPage5Char = 0;
            lastPage5Char = 0;
            nbPage5Char = 0;
        }
    }
    class CWorker : System.Threading.WaitHandle
    {
        public int iWorker;
        public String tempDir;
        CBinarNASList oInfo;
        private bool bError;
        //public String subfolder;

        //static private System.Threading.Mutex mutexBIG = new System.Threading.Mutex();
        //static private Boolean bBigInUse = false;
        public enum EStatus
        {
            idle = 0,
            working = 1,
            ended = 2,
        }
        public EStatus status = EStatus.idle;

        System.Text.StringBuilder stdOut = null;
        System.Text.StringBuilder stdErr = null;
        //private string outFile;
        //private string outDir;
        private bool bBig;

        private int currentPageProcessing = -1;
        private System.IO.FileInfo[] tabJP2Files;
        private System.IO.DirectoryInfo oDirSource;
        private System.IO.FileInfo[] tabPDFSource;

        Process exeProcess;

        public CWorker(int paramWorker)
        {
            iWorker = paramWorker;
            tempDir = Program.TEMP_ROOT + iWorker.ToString();
        }

        public void TryNext()
        {
            if (status == CWorker.EStatus.working)
            {
                FinishProcess();
            }
            if (status == CWorker.EStatus.working) return;

            bError = false;
            String sql = "select top 1 " + CBinarNASList.FIELD_LIST;
            sql += " from BinarNASList where";
            sql += " nbPageJP2 = nbPagePDFOrig and id > @lastid";
            if (Program.bTest)
                sql += Program.TEST_RESTRICT;
            else
                sql += Program.TEST_DATE_DONE_PROD;
            bBig = false;
            if (iWorker > 1)
            {
                sql += " and (not " + Program.SQL_TEST_BIG + ")";
            }
            else if (Program.bStillBig)
            {
                sql += " and " + Program.SQL_TEST_BIG;
                bBig = true;
            }

            if (Program.nDone > Program.MAX_DONE) sql += " and 1=0";
            sql += " order by id asc";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                int thisLastID;
                if (bBig) thisLastID = Program.lastIdBig;
                else thisLastID = Program.lastId;
                Debug.Print(sql + ", lastid=" + thisLastID);
                cmd.Parameters.AddWithValue("@lastid", thisLastID);
                using (System.Data.SqlClient.SqlDataReader rs = cmd.ExecuteReader())
                {
                    if (!rs.Read())
                    {
                        if (bBig && Program.bStillBig && !Program.B_TEST_FOLDER)
                        {
                            Program.bStillBig = false;
                            oInfo = null;
                        }
                        //Console.WriteLine("No more for\n" + sql + "\n" + Program.LogBinds(cmd.Parameters));
                        status = EStatus.ended;
                        return;
                    }
                    oInfo = new CBinarNASList();
                    oInfo.LoadFromRS(rs);
                    if (bBig) Program.lastIdBig = oInfo.id;
                    else Program.lastId = oInfo.id;
                }
                if (oInfo == null)
                {
                    this.TryNext();
                    return;
                }
            }

            //outDir = Program.Output + oInfo.subFolder;
            //string inFiles;
            oDirSource = new System.IO.DirectoryInfo(oInfo.folder);
            Console.Write(oInfo.subFolder + "\\" + oInfo.basename + "_*.jp2                  \r");
            if (!oDirSource.Exists)
            {
                Program.LogErrorSQL("inexistant folder", "", oInfo.basename, oInfo.box, oInfo.Seqno ?? 0, oInfo.Lang, oInfo.Joker ?? 0, oInfo.id);
                status = EStatus.idle;
                return;
            }
            tabPDFSource = oDirSource.GetFiles(oInfo.basename + "_*.pdf");
            if (tabPDFSource.Length != oInfo.nbPageJP2)
            {
                Program.LogErrorSQL("bad PDF number", tabPDFSource.Length.ToString() + " instead of " + oInfo.nbPageJP2.ToString(), oInfo.folder + "\\" + oInfo.basename + "_ *.jp2", oInfo.box, oInfo.Seqno ?? 0, oInfo.Lang, oInfo.Joker ?? 0, oInfo.id);
                status = EStatus.idle;
                return;
            }
            tabJP2Files = oDirSource.GetFiles(oInfo.basename + "_*.jp2");
            if (tabJP2Files.Length != oInfo.nbPageJP2)
            {
                Program.LogErrorSQL("bad JP2 number", tabJP2Files.Length.ToString() + " instead of " + oInfo.nbPageJP2.ToString(), oInfo.folder + "\\" + oInfo.basename + "_ *.jp2", oInfo.box, oInfo.Seqno ?? 0, oInfo.Lang, oInfo.Joker ?? 0, oInfo.id);
                status = EStatus.idle;
                return;
            }
            if (oInfo.nbMultipage > 0)
            {
                // remove POM files
                int nbRemovedPDF = RemovePOM(ref tabPDFSource);
                int nbRemovedJP2 = RemovePOM(ref tabJP2Files);
                if (nbRemovedJP2 != nbRemovedPDF)
                    Program.LogErrorSQL("POM nb", "nb POM: " + nbRemovedJP2 + " JP2, " + nbRemovedPDF + " PDF", oInfo.folder + "\\" + oInfo.basename + "_ *.jp2", oInfo.box, oInfo.Seqno ?? 0, oInfo.Lang, oInfo.Joker ?? 0, oInfo.id);
            }
            tabJP2Files.OrderBy(Name => Name);
            if (Program.B_TEST_FOLDER)
            {
                status = EStatus.idle;
                return;
            }

            if (System.IO.Directory.Exists(tempDir)) System.IO.Directory.Delete(tempDir, true);
            System.IO.Directory.CreateDirectory(tempDir);
            currentPageProcessing = -1;
            status = EStatus.working;
            LaunchConvertOnePage();
        }

        static public int RemovePOM(ref System.IO.FileInfo[] tab)
        {
            var l = new List<System.IO.FileInfo>();
            int nbRemoved = 0;
            foreach (var o in tab)
            {
                if (o.Name.Contains("POM"))
                {
                    nbRemoved++;
                }
                else
                {
                    l.Add(o);
                }
            }
            tab = l.ToArray();
            return nbRemoved;
        }

        private void LaunchConvertOnePage()
        {
            currentPageProcessing++;
            if (currentPageProcessing >= tabJP2Files.Length)
            {   // done with image convertion, do OCR merge
                MergeOCR();
                return;
            }

            stdOut = new System.Text.StringBuilder();
            stdErr = new System.Text.StringBuilder();
            ProcessStartInfo startInfo = new ProcessStartInfo
            {
                CreateNoWindow = false,
                UseShellExecute = false,
                FileName = Program.IMAGEMAGICK_FOLDER + "\\convert.exe",
                WindowStyle = ProcessWindowStyle.Hidden,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            string args = Program.CONVERT_PARAMS + " \"" + tabJP2Files[currentPageProcessing].FullName + "\" "
                + tempDir + "\\page" + currentPageProcessing.ToString("000000") + ".pdf";

#if false   // test speed conversion for whole doc
            if (currentPageProcessing == 0)
                args = Program.CONVERT_PARAMS + " \"" + oInfo.folder + "\\" + oInfo.basename + "_*.jp2" + "\" "
                    + tempDir + "\\page" + currentPageProcessing.ToString() + ".pdf";
#endif
            startInfo.Arguments = args;

            exeProcess = new Process();
            exeProcess.OutputDataReceived += new DataReceivedEventHandler(OutHandler);
            exeProcess.ErrorDataReceived += new DataReceivedEventHandler(ErrHandler);
            exeProcess.StartInfo = startInfo;
            //String msg = "Worker no " + iWorker.ToString() + " " + box + " " + doc + " " + subfolder;
            //if (bBig) msg += " (big)";
            //msg += " (" + nbPage.ToString() + "p. " + maxSizeM + "M)";
            Console.WriteLine("Start convert" + iWorker.ToString() + " id=" + oInfo.id.ToString() +
                (oInfo.Seqno.HasValue ? " Seqno=" + oInfo.Seqno.ToString() : " Joker=" + oInfo.Joker.ToString()) +
                (bBig ? " big" : " small") + " " +
                oInfo.subFolder + "\\" + tabJP2Files[currentPageProcessing].Name + " page " + currentPageProcessing.ToString());
            this.Log("Start convert page " + currentPageProcessing.ToString(), args, false);
            exeProcess.Start();
            exeProcess.BeginOutputReadLine();
            exeProcess.BeginErrorReadLine();
            this.SafeWaitHandle = new Microsoft.Win32.SafeHandles.SafeWaitHandle(exeProcess.Handle, false);
        }

        private void MergeOCR()
        {
            if (!bError)
            {
                System.IO.Directory.CreateDirectory(oInfo.outDir);
                var lBlankPages = new List<int>();
                String sql = "select page from BinarPage where id=@id and " +
                    "isNull(Blank, case when nbCharTrim <= " + Program.LIMIT_CHAR_PAGE_BLANK.ToString() + " then 1 else 0 end) = 1";
                using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
                {
                    cmd.Parameters.AddWithValue("@id", oInfo.id);
                    using (var rs = cmd.ExecuteReader())
                    {
                        while (rs.Read()) lBlankPages.Add(rs.GetInt32(0));
                    }
                }
                Program.DoMergeOCR(oInfo.folder, oInfo.basename + "_*.pdf", oInfo.target, tempDir, oInfo.id, lBlankPages);
            }

            if (!bError) oInfo.MarkDone();
            Console.WriteLine("id=" + oInfo.id + ", bError=" + bError.ToString());

            Program.nDone++;
            status = EStatus.idle;
        }

        public void FinishProcess()
        {
            if (!exeProcess.HasExited) return;
            exeProcess.WaitForExit();
            String sError = null;
            if (stdOut.Length > 0)
            {
                this.Log("Compress stdout", stdOut.ToString(), true);
            }
            if (stdErr.Length > 0)
            {
                bError = true;
                sError += " " + stdErr;
                this.Log("Compress stderr", stdErr.ToString(), true);
            }
            if (exeProcess.ExitCode != 0)
            {
                this.Log("Compress err", " Result compress=" + exeProcess.ExitCode, true);
                bError = true;
                sError += " " + "compress exit=" + exeProcess.ExitCode;
            }
            /*
            String targetPath;
            if (String.IsNullOrEmpty(sError))
            {
                //targetPath = outDir + "\\" + oInfo.basename + ".bin.pdf";
            }
            else
            {
                //targetPath = outDir + "\\" + oInfo.basename + ".error.pdf";
                using (System.IO.StreamWriter streamError = System.IO.File.CreateText(targetPath))
                {
                    streamError.Write(sError);
                }
            }
            */
            //Console.WriteLine("End convert " + (bBig ? "big" : "small") + " " + oInfo.subFolder);
            this.Log("End Worker", sError, false);
            LaunchConvertOnePage();
        }

        void Log(String key, String msg, bool bConsole)
        {
            Program.Log(key, msg, oInfo.subFolder, oInfo.box, oInfo.Seqno, oInfo.Lang, oInfo.Joker, iWorker, bConsole, oInfo.id);
        }

        void OutHandler(object sendingProcess, DataReceivedEventArgs outLine)
        {
            if (outLine.Data == null) return;
            if (outLine.Data.Trim() == "") return;
            stdOut.Append(outLine.Data);
            stdOut.Append("\n");
        }
        void ErrHandler(object sendingProcess, DataReceivedEventArgs outLine)
        {
            if (outLine.Data == null) return;
            if (outLine.Data.Trim() == "") return;
            stdErr.Append(outLine.Data);
            stdErr.Append("\n");
        }
    }

}

public class CXY
{
    public int X0, X1, Y0, Y1;
}

public class PdfCanvasRemoveImages2 : UnesdocBatchConvert.PdfCanvasEditor2
{
    //private List<String> lResources2Remove;

    protected override void Write(PdfCanvasProcessor processor, PdfLiteral @oOperator, IList<PdfObject> operands)
    {
        String operatorString = oOperator.ToString();

        //if (operatorString.Substring(0, 1) == "Do")
        if (operatorString == "Do")
        {
            if (operands.Count >= 1 && operands[0] is PdfName)
            {
                //String ResourceName = operands[0].ToString();
                //lResources2Remove.Add(ResourceName);
                //Console.WriteLine("drop " + operatorString + " " + ResourceName);
                return;
            }
            /*
        foreach (var o in operands)
        {
            Console.WriteLine("operand " + o.GetType() + " " + o.GetObjectType() + " " + o.ToString());
        }
            */
        }

        //Console.WriteLine("keep " + operatorString);
        base.Write(processor, oOperator, operands);
    }

    public override void PreEditPage(PdfPage pageIn, PdfPage pageOut, PdfDocument oDocumentIn, int pageNumberIn, PdfDocument oDocumentOut, int pageNumberOut)
    {
    }

    //static readonly PdfName PdfnameXObject = new PdfName("XObject");

    public override void PostEditPage(PdfPage pageIn, PdfPage pageOut, PdfDocument oDocumentIn, int pageNumberIn, PdfDocument oDocumentOut, int pageNumberOut)
    {
        /*
        var oldResources = page.GetResources();

        var dicResource = oldResources.GetPdfObject();
        dicResource.Remove(PdfnameXObject); // remove images
        */
    }

}
public class PdfCanvasRemoveImages : UnesdocBatchConvert.PdfCanvasEditor
{
    //private List<String> lResources2Remove;

    protected override void Write(PdfCanvasProcessor processor, PdfLiteral @oOperator, IList<PdfObject> operands)
    {
        String operatorString = oOperator.ToString();

        //if (operatorString.Substring(0, 1) == "Do")
        if (operatorString == "Do")
        {
            if (operands.Count >= 1 && operands[0] is PdfName)
            {
                //String ResourceName = operands[0].ToString();
                //lResources2Remove.Add(ResourceName);
                //Console.WriteLine("drop " + operatorString + " " + ResourceName);
                return;
            }
            /*
        foreach (var o in operands)
        {
            Console.WriteLine("operand " + o.GetType() + " " + o.GetObjectType() + " " + o.ToString());
        }
            */
        }

        //Console.WriteLine("keep " + operatorString);
        base.Write(processor, oOperator, operands);
    }

    public override void PreEditPage(PdfPage page, PdfDocument oDocument, int pageNumber)
    {
        //lResources2Remove = new List<String>();
    }

    /*
    static readonly PdfName[] LIST_TYPE_RESOURCE = { PdfName.ColorSpace, PdfName.ExtGState, PdfName.Pattern, PdfName.Shading, PdfName.XObject, PdfName.Font };
    enum EIndexTypeResource
    {
        ColorSpace = 0,
        ExtGState,
        Pattern,
        Shading,
        XObject,
        Font,
        max = Font,
    }
    */
    //static readonly PdfName PdfnameXObject = new PdfName("XObject");
    //PdfObject objFontFirstpage;

    public override void PostEditPage(PdfPage page, PdfDocument oDocument, int pageNumber)
    {
        var oldResources = page.GetResources();
        //var newResources = new PdfResources();

        var dicResource = oldResources.GetPdfObject();

        /* *
        if (pageNumber > 0)  foreach (var nameResource in oldResources.GetResourceNames(PdfName.Font))
        {
            Console.WriteLine("font " + nameResource.ToString());
        }
        // */

        /* *
        foreach (var r in dicResource.KeySet())
        {
            Console.WriteLine("ResourceDictionary key " + r.ToString());
        }
        // */
        dicResource.Remove(PdfName.XObject); // remove images

#if false // try to set same font (but fail)
        if (pageNumber == 1)
        {
            objFontFirstpage = dicResource.Get(PdfName.Font);
        }
        else
        {
            dicResource.Remove(PdfName.Font);
            dicResource.Put(PdfName.Font, objFontFirstpage);
        }
        //dicResource.Remove(PdfName.Font); // remove fonts
#endif

        /*
        for (int iTypeResource = 0; iTypeResource <= (int)EIndexTypeResource.max; iTypeResource++)
        {
            PdfName nameTypeResource = LIST_TYPE_RESOURCE[iTypeResource];
            foreach (var nameResource in oldResources.GetResourceNames(nameTypeResource))
            {
                if (lResources2Remove.Contains(nameResource.ToString()))
                {
                    Console.WriteLine("drop resource " + nameTypeResource.ToString() + " " + nameResource.ToString());
                    continue;
                }
                Console.WriteLine("keep resource " + nameTypeResource.ToString() + " " + nameResource.ToString());

                switch (iTypeResource)
                {
                    case (int)EIndexTypeResource.ColorSpace:
                        newResources.AddColorSpace(oldResources.GetColorSpace(nameResource));
                        break;
                    case (int)EIndexTypeResource.ExtGState:
                        newResources.AddExtGState(oldResources.GetPdfExtGState(nameResource));
                        break;
                    case (int)EIndexTypeResource.Pattern:
                        newResources.AddPattern(oldResources.GetPattern(nameResource));
                        break;
                    case (int)EIndexTypeResource.Shading:
                        newResources.AddShading(oldResources.GetShading(nameResource));
                        break;
                    case (int)EIndexTypeResource.XObject:
                        //throw new Exception("unable to manage resource XObject");
                        Console.WriteLine("unable to manage resource XObject");
                        break;
                    case (int)EIndexTypeResource.Font:
                        //newResources.AddFont();
                        //throw new Exception("unable to manage resource Font");
                        Console.WriteLine("unable to manage resource Font");
                        break;
                    default:
                        throw new Exception("bad type resource index");
                }
            }
        }
        page.SetResources(newResources);
        // */
    }

}

public class TrueTypeFontWName : TrueTypeFont
{
    public TrueTypeFontWName(String filePath) : base(filePath)
    {
    }

    public void SetFontName2(String fontName)
    {
        SetFontName(fontName);
    }
}