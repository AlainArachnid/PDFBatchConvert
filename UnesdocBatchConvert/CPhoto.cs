#define PDFA
//#define REPLACEFONT

using System;
using System.IO;
using System.Collections.Generic;
using iText.Kernel.Pdf;
using iText.Kernel.Utils;
using iText.Layout;
using iText.Layout.Element;
using System.Linq;
using System.Web;

namespace UnesdocBatchConvert
{
    public class CPhoto
    {
        const String ROOTSOURCE = @"\\somePath\PHOTO";
        const String ROOTTARGET = @"D:\otherPath\photos";

        private readonly int _id;
        private String _path0;
        private String _path1;
        private long _lg0;
        private long _lg1;
        private int? _nMaxPage;
        private DateTime? _dateDone;
        private readonly int _rotate;

        public const String FIELD_LIST = "[Id], [Path0], [Path1], [SizePdf0], [SizePdf1], [maxPage], doneDate, rotate";
        public enum ERs : int
        {
            id = 0,
            Path0,
            Path1,
            SizePdf0,
            SizePdf1,
            maxPage,
            dateDone,
            rotate,
        }

        public static void DoScan()
        {
            var dirRoot = new DirectoryInfo(CPhoto.ROOTSOURCE);
            foreach (var dir2 in dirRoot.GetDirectories())
            {
                var dPhoto = new Dictionary<int, CPhoto>();
                foreach (var pdffile in dir2.GetFiles("*.pdf"))
                {
                    // PHOTO0000005245_0001.pdf
                    if (!int.TryParse(pdffile.Name.Substring(5, 10), out int id))
                    {
                        Program.LogErrorSQL("BadPhoto", "no id", pdffile.FullName, null, 0, null, 0, 0);
                        //Console.WriteLine(pdffile.Name.Substring(5, 10));
                        //return;
                        continue;
                    }
                    if (!int.TryParse(pdffile.Name.Substring(16, 4), out int iVerso))
                    {
                        Program.LogErrorSQL("BadPhoto", "no recto/verso", pdffile.FullName, null, 0, null, 0, 0);
                        //Console.WriteLine(pdffile.Name.Substring(16, 4));
                        //return;
                        continue;
                    }
                    String NameShouldBe = "PHOTO" + id.ToString("0000000000") + "_" + iVerso.ToString("0000") + ".pdf";
                    if (string.Compare(pdffile.Name, NameShouldBe, true) != 0)
                    {
                        Program.LogErrorSQL("BadPhoto", "Bad name, should be " + NameShouldBe, pdffile.FullName, null, 0, null, 0, 0);
                        //Console.WriteLine("should be " + NameShouldBe);
                        //Console.WriteLine("is        " + pdffile.Name);
                        //return;
                        continue;
                    }
                    if (iVerso < 0 || iVerso > 3)
                    {
                        Program.LogErrorSQL("BadPhoto", "Bad iVerso " + iVerso.ToString(), pdffile.FullName, null, 0, null, 0, 0);
                        continue;
                    }
                    Console.Write(id.ToString() + "   \r");
                    //String sql = "select id from PhotoNASList where id=@id and ";
                    //if (iVerso > 0) sql += "Path0"
                    if (!dPhoto.TryGetValue(id, out CPhoto oPhoto))
                    {
                        oPhoto = new CPhoto(id);
                        dPhoto.Add(id, oPhoto);
                    }
                    oPhoto.SetData(iVerso, pdffile);
                }
                using (var cmd = new System.Data.SqlClient.SqlCommand())
                {
                    cmd.Connection = Program.conn;
                    var sql = new System.Text.StringBuilder();
                    sql.Append("insert into PhotoNASList(id, Path0, Path1, SizePDF0, SizePdf1, maxPage) values ");
                    int iArg = 1;
                    foreach (var kv in dPhoto)
                    {
                        if (!kv.Value.IsValid()) continue;
                        if (iArg > 1) sql.Append(", ");
                        sql.Append("(@id" + iArg + ", @p0_" + iArg + ", @p1_" + iArg + ", @l0_" + iArg + ", @l1_" + iArg + ", @n" + iArg + ")");
                        cmd.Parameters.AddWithValue("@id" + iArg, kv.Value._id);
                        cmd.Parameters.AddWithValue("@p0_" + iArg, kv.Value._path0);
                        cmd.Parameters.AddWithValue("@p1_" + iArg, kv.Value._path1);
                        cmd.Parameters.AddWithValue("@l0_" + iArg, kv.Value._lg0);
                        cmd.Parameters.AddWithValue("@l1_" + iArg, kv.Value._lg1);
                        if (kv.Value._nMaxPage.HasValue)
                            cmd.Parameters.AddWithValue("@n" + iArg, kv.Value._lg1);
                        else
                            cmd.Parameters.AddWithValue("@n" + iArg, DBNull.Value);
                        iArg++;
                    }
                    cmd.CommandText = sql.ToString();
                    cmd.ExecuteNonQuery();
                }
            }
            Console.WriteLine("");
        }

        public static void DoTestOne()
        {
            Program.InitICM();
            String sql = "select top 1 " + FIELD_LIST + " from PhotoNASList where id=6";
            CPhoto oOne;
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                using (var rs = cmd.ExecuteReader())
                {
                    rs.Read();
                    oOne = new CPhoto(rs);

                }
            }
            oOne.DoMerge();
        }

        public static void DoTestFour()
        {
            Program.InitICM();
            String sql = "select " + FIELD_LIST + " from PhotoNASList where id in (2, 3336, 924, 2889)";
            var listPhoto = new List<CPhoto>();
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                using (var rs = cmd.ExecuteReader())
                {
                    while (rs.Read()) listPhoto.Add(new CPhoto(rs));
                }
            }
            foreach (var oPhoto in listPhoto) oPhoto.DoMerge();
        }

        public static void DoMergeAll()
        {
            Program.InitICM();
            String sql = "select " + FIELD_LIST + " from PhotoNASList where donedate is null or donedate < '2023-09-06'";
            var listPhoto = new List<CPhoto>();
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                using (var rs = cmd.ExecuteReader())
                {
                    while (rs.Read()) listPhoto.Add(new CPhoto(rs));
                }
            }
            foreach (var oPhoto in listPhoto) oPhoto.DoMerge();
        }

        public void DoMerge()
        {
            var listPathSource = new List<String>();
            // Special cases
            if (_id == 2889)
            {
                var oPhotoBis = new CPhoto(2888);
                oPhotoBis.SetData(0, new System.IO.FileInfo(ROOTSOURCE + @"\" + _path0.Replace("_0000", "_0002")));
                oPhotoBis.SetData(1, new System.IO.FileInfo(ROOTSOURCE + @"\" + _path1.Replace("_0001", "_0003")));
                oPhotoBis.DoMerge();
                // continue for 2 other pages
                _nMaxPage = null;
            }
            if (_nMaxPage.HasValue)
            {
                listPathSource.Add(ROOTSOURCE + @"\" + _path0.Replace("_0000", "_0002"));
                listPathSource.Add(ROOTSOURCE + @"\" + _path0);
                listPathSource.Add(ROOTSOURCE + @"\" + _path1);
            }
            else
            {
                listPathSource.Add(ROOTSOURCE + @"\" + _path1);
                listPathSource.Add(ROOTSOURCE + @"\" + _path0);
            }
            DoMerge(listPathSource);
        }

        public void DoMerge(List<String> listPathSource)
        {
            var pathTarget = ROOTTARGET + @"\" + _id.ToString("0000") + ".pdf";
            using (var writerTarget = new iText.Kernel.Pdf.PdfWriter(pathTarget))
#if PDFA
            using (var readICM = new MemoryStream(Program.icm, false))
            using (var docTarget = new iText.Pdfa.PdfADocument(writerTarget, PdfAConformanceLevel.PDF_A_2B, new PdfOutputIntent
                ("Custom", "", "http://www.color.org", "sRGB IEC61966-2.1", readICM)))
#else
            using (var docTarget = new PdfDocument(writerTarget))
#endif
            {
#if REPLACEFONT
                using (var doc = new Document(docTarget))
                {
                    var font = GlyphLessFont.AddToPdfDocument(docTarget, Program.FONT_FILE_GLYPHLESS);
                    doc.SetFont(font);
#endif
                //using (var doc = new Document(docTarget))
                //{
                //var merger = new PdfMerger(docTarget);
                //merger.SetCloseSourceDocuments(true);
                int iPage = 1;
                foreach (var pathSourcePage in listPathSource)
                {
                    using (var readerOrig = new iText.Kernel.Pdf.PdfReader(pathSourcePage))
                    using (var docIn = new iText.Kernel.Pdf.PdfDocument(readerOrig))
                    {
                        //merger.Merge(docIn, 1, iPage);
                        docIn.CopyPagesTo(1, 1, docTarget);
                        if (iPage == 1 && _rotate != 0)
                        {
                            var oPage = docTarget.GetPage(1);
                            int rotateOrig = oPage.GetRotation();
                            if (rotateOrig == 0)
                            {
                                oPage.SetRotation(_rotate);
                            }
                            else
                            {
                                oPage.SetRotation((rotateOrig + _rotate) % 360);
                            }
                        }
                    }
                    iPage++;
                }
                //}
#if REPLACEFONT
                }
#endif
                Program.DoMergeFontsDoc(docTarget, _id);
                docTarget.Close();
            }

            _dateDone = DateTime.Now;
            UpdateDateDone();
        }


        public CPhoto(int paramId)
        {
            _id = paramId;
        }

        public CPhoto(System.Data.SqlClient.SqlDataReader rs)
        {
            _id = rs.GetInt32((int)ERs.id);
            _path0 = rs.GetString((int)ERs.Path0);
            _path1 = rs.GetString((int)ERs.Path1);
            _lg0 = rs.GetInt64((int)ERs.SizePdf0);
            _lg1 = rs.GetInt64((int)ERs.SizePdf1);
            if (!rs.IsDBNull((int)ERs.maxPage)) _nMaxPage = rs.GetInt32((int)ERs.maxPage);
            if (!rs.IsDBNull((int)ERs.dateDone)) _dateDone = rs.GetDateTime((int)ERs.dateDone);
            _rotate = 0;
            if (!rs.IsDBNull((int)ERs.rotate)) _rotate = rs.GetInt32((int)ERs.rotate);
        }

        public void UpdateDateDone()
        {
            String sql = "update PhotoNASList set doneDate=@d where id=@id";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                cmd.Parameters.AddWithValue("@d", _dateDone);
                cmd.Parameters.AddWithValue("@id", _id);
                cmd.ExecuteNonQuery();
            }
        }
        public Boolean IsValid()
        {
            if (_id <= 0)
            {
                Program.LogErrorSQL("BadPhoto", "id null", null, null, 0, null, 0, 0);
                return false;
            }
            if (_path0 == null)
            {
                Program.LogErrorSQL("BadPhoto", "path0 null", null, null, 0, null, 0, 0);
                return false;
            }
            if (_path1 == null)
            {
                Program.LogErrorSQL("BadPhoto", "path1 null", null, null, 0, null, 0, 0);
                return false;
            }
            if (_lg0 <= 0)
            {
                Program.LogErrorSQL("BadPhoto", "lg0 null", _path0, null, 0, null, 0, 0);
                return false;
            }
            if (_lg1 <= 0)
            {
                Program.LogErrorSQL("BadPhoto", "lg1 null", _path1, null, 0, null, 0, 0);
                return false;
            }
            return true;
        }

        public void SetData(int iVerso, System.IO.FileInfo pdffile)
        {
            switch (iVerso)
            {
                case 0:
                    if (_path0 != null)
                    {
                        Program.LogErrorSQL("BadPhoto", "Double id= " + _id.ToString() + ", iVerso=" + iVerso.ToString() + " " + _path0, pdffile.FullName, null, 0, null, 0, 0);
                        return;
                    }
                    _path0 = pdffile.FullName.Substring(ROOTSOURCE.Length + 1);
                    _lg0 = pdffile.Length;
                    break;
                case 1:
                    if (_path1 != null)
                    {
                        Program.LogErrorSQL("BadPhoto", "Double id= " + _id.ToString() + ", iVerso=" + iVerso.ToString() + " " + _path1, pdffile.FullName, null, 0, null, 0, 0);
                        return;
                    }
                    _path1 = pdffile.FullName.Substring(ROOTSOURCE.Length + 1);
                    _lg1 = pdffile.Length;
                    break;
                case 2:
                case 3:
                    if (_nMaxPage.HasValue)
                    {
                        if (_nMaxPage < iVerso) _nMaxPage = iVerso;
                    }
                    else
                    {
                        _nMaxPage = iVerso;
                    }
                    break;
                default:
                    Program.LogErrorSQL("BadPhoto", "Bad iVerso=" + iVerso.ToString(), pdffile.FullName, null, 0, null, 0, 0);
                    return;
            }
        }
    }
}

