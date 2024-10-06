//#define TRACE_ANA_ID

using System;
using System.IO;
using System.Collections.Generic;
using iText.Kernel.Pdf;
using iText.Kernel.Utils;
using iText.Layout;
using iText.Layout.Element;
using System.Linq;
using System.Web;
using System.Text.RegularExpressions;

namespace UnesdocBatchConvert
{
    public class CPortrait
    {
        const String ROOTSOURCE = @"\\somePath\PHOTO_PORTRAITS";
        const String ROOTTARGET = @"D:\otherPath\portraits";

        private readonly int _id;
        private readonly int _id1;
        private readonly int _id2;
        private int _id3;
        private readonly String _folder;
        private readonly String _filename;
        private readonly String _textBefore;
        private readonly String _textAfter;
        private readonly long _lg;
        private DateTime? _dateDone;
        private readonly String _MD5;
        private readonly int _doubleOf;
        public string lastname;
        public string firstname;
        public string place;
        public string datePict;
        public int seqnoName;

        static private Regex _reSplitWords;
        static private Regex _reRemove_xxA;

        public const String FIELD_LIST = "Id1, Id2, Id3, Folder, Filename, textBefore, textAfter, SizeJPG, doneDate, MD5, DoubleOf, id, Lastname, Firstname, Place, Datepict, SeqnoName";
        public enum ERs : int
        {
            id1 = 0,
            id2,
            id3,
            Folder,
            Filename,
            textBefore,
            textAfter,
            SizeJPG,
            dateDone,
            MD5,
            doubleOf,
            id,
            lastname,
            firstname,
            place,
            datepict,
            seqnoName,
        }

        public static void DoScan()
        {
            Console.ReadLine();
            using (var cmd = new System.Data.SqlClient.SqlCommand("truncate table PortraitNASList", Program.conn))
            {
                cmd.ExecuteNonQuery();
            }
            using (var cmd = new System.Data.SqlClient.SqlCommand("truncate table PortraitNASDouble", Program.conn))
            {
                cmd.ExecuteNonQuery();
            }
            var dirRoot = new DirectoryInfo(CPortrait.ROOTSOURCE);
            DoScan(dirRoot);
            Console.WriteLine("");
        }

        public static void TestOneScan()
        {
            var oFile = new System.IO.FileInfo(@"\\hq-Synav-fon\Digital_Archives\Digitizing shared UNESCO History (2017-2019)\PHOTO_PORTRAITS\L\Lustiger, Cardinal    Paris Unesco 27 Octobre 1998  70007630 (11).jpg");
            var oPortrait = new CPortrait(oFile);
            Console.WriteLine("");
            Console.WriteLine(oFile.FullName);
            Console.WriteLine(oPortrait);
        }

        public static void DoScan(DirectoryInfo oDir)
        {
            foreach (var dir2 in oDir.GetDirectories())
            {
                if (String.Compare(dir2.Name, "vignettes", true) == 0) continue;
                DoScan(dir2);
            }
            foreach (var oFile in oDir.GetFiles())
            {
                switch (oFile.Extension.ToLower())
                {
                    case ".jpg":
                        var oPortrait = new CPortrait(oFile);
                        //oPortrait.CheckDouble();
                        /*
                        if (!String.IsNullOrEmpty(otherPath))
                        {
                            String sql = "insert into PortraitNASDouble(Path1, Path2) values (@p1, @p2)";
                            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
                            {
                                cmd.Parameters.AddWithValue("@p1", oFile.FullName);
                                cmd.Parameters.AddWithValue("@p2", otherPath);
                                cmd.ExecuteNonQuery();
                            }
                            Program.LogErrorSQL("BadPortrait", "Double", oFile.FullName, null, 0, null, 0);
                            continue;
                        }
                        */
                        oPortrait.SQLInsert();
                        break;
                    case ".db":
                    case ".xls":
                    case ".doc":
                        // ignore
                        break;
                    default:
                        Program.LogErrorSQL("BadPortrait", "extension = " + oFile.Extension, oFile.FullName, null, 0, null, 0, 0);
                        break;
                }
            }
        }

        public static void DoTestOne()
        {
            String sql = "select top 1 " + FIELD_LIST + " from PortraitNASList";
            CPortrait oOne;
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                using (var rs = cmd.ExecuteReader())
                {
                    rs.Read();
                    oOne = new CPortrait(rs);

                }
            }
            oOne.DoProcess();
        }

        public static void DoTestFour()
        {
            String sql = "select " + FIELD_LIST + " from PortraitNASList where id in (2, 3336, 924, 2889)";
            var listPhoto = new List<CPortrait>();
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                using (var rs = cmd.ExecuteReader())
                {
                    while (rs.Read()) listPhoto.Add(new CPortrait(rs));
                }
            }
            foreach (var oPhoto in listPhoto) oPhoto.DoProcess();
        }

        public static void DoProcessAll()
        {
            String sql = "select " + FIELD_LIST + " from PortraitNASList where donedate is null";
            var listPhoto = new List<CPortrait>();
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                using (var rs = cmd.ExecuteReader())
                {
                    while (rs.Read()) listPhoto.Add(new CPortrait(rs));
                }
            }
            foreach (var oPhoto in listPhoto) oPhoto.DoProcess();
        }

        public static void InitNames()
        {
            _reSplitWords = new System.Text.RegularExpressions.Regex(@"^(\s+|\d+|[\w'-]+|[^\d\s\w'-]+)+$");
            _reRemove_xxA = new Regex(@"^\(\d+[ABab]*\)\s*");
            String sql = "update PortraitNASList set lastname=null, firstname=null, place=null, datePict=null";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn)) cmd.ExecuteNonQuery();
            sql = "select " + FIELD_LIST + " from PortraitNASList where donedate is null";
            //sql += " and md5='791995B296FB327AC9DACC13568928D1'";
            var listMD5 = new Dictionary<String, List<CPortrait>>();
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                using (var rs = cmd.ExecuteReader())
                {
                    while (rs.Read())
                    {
                        var oPortrait = new CPortrait(rs);
                        if (!listMD5.TryGetValue(oPortrait.MD5, out List<CPortrait> lThisMD5))
                        {
                            lThisMD5 = new List<CPortrait>() { oPortrait };
                            listMD5.Add(oPortrait.MD5, lThisMD5);
                        }
                        else
                        {
                            lThisMD5.Add(oPortrait);
                        }

                    }
                }
            }
            foreach (var kvMD5 in listMD5)
            {
                String Lastname = null;
                String Firstname = null;
                String Place = null;
                String DatePict = null;
                foreach (var oPortrait in kvMD5.Value)
                {
                    oPortrait.AnalyseNames();
                    MergeName(ref Lastname, oPortrait.lastname, "id=" + oPortrait.ID);
                    MergeName(ref Firstname, oPortrait.firstname, "id=" + oPortrait.ID);
                    MergeName(ref Place, oPortrait.place, "id=" + oPortrait.ID);
                    MergeName(ref DatePict, oPortrait.datePict, "id=" + oPortrait.ID);
                    //Console.WriteLine(oPortrait._textBefore + "--" + oPortrait._textAfter + "=>" + Firstname + "--" + Lastname + "--" + Place + "--" + DatePict);
                }
                //return;
                if (TRACE_NAMES) Console.WriteLine(Firstname + "--" + Lastname + "--" + Place + "--" + DatePict);
                sql = "update PortraitNASList set Lastname=@l, Firstname=@f, Place=@p, DatePict=@d where MD5=@md5";
                using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
                {
                    if (Lastname == null || Lastname == "")
                        cmd.Parameters.AddWithValue("@l", DBNull.Value);
                    else
                        cmd.Parameters.AddWithValue("@l", Lastname);
                    if (Firstname == null || Firstname == "")
                        cmd.Parameters.AddWithValue("@f", DBNull.Value);
                    else
                        cmd.Parameters.AddWithValue("@f", Firstname);
                    if (Place == null || Place == "")
                        cmd.Parameters.AddWithValue("@p", DBNull.Value);
                    else
                        cmd.Parameters.AddWithValue("@p", Place);
                    if (DatePict == null || DatePict == "")
                        cmd.Parameters.AddWithValue("@d", DBNull.Value);
                    else
                        cmd.Parameters.AddWithValue("@d", DatePict);
                    cmd.Parameters.AddWithValue("@md5", kvMD5.Key);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        void AnalyseNames()
        {
            AnalyseName(_textAfter);
            AnalyseName(_textBefore);
        }

        enum EEtatName : int
        {
            init = 0,
            postComma,
            inPlace,
            inDate,
        }

        static readonly bool TRACE_NAMES = false;
        void AnalyseName(String sName)
        {
            if (sName == null || sName == "") return;
            if (TRACE_NAMES) Console.WriteLine("*** " + sName);

            String sName2 = _reRemove_xxA.Replace(sName, "");
            if (TRACE_NAMES && sName2 != sName) Console.WriteLine(sName + "==>" + sName2);

            var match = _reSplitWords.Match(sName2);
            //String Lastname = null;
            //String Firstname = null;
            //String Place = null;
            //String Datepict = null;
            EEtatName eEtat = EEtatName.init;
            var lCurrent = new List<String>();
            var lLastName = new List<String>();
            var lFirstName = new List<String>();
            var lPlace = new List<String>();
            var lDatepict = new List<String>();

            foreach (Capture capture in match.Groups[1].Captures)
            {
                String s = capture.Value;
                if (s.Trim() == "") continue;
                if (TRACE_NAMES) Console.WriteLine(s);
                //if (s == ".") continue;
                if ((s.IndexOf(".") >= 0 || IsTitle(s)) && eEtat == EEtatName.init)
                //if (IsTitle(s))
                {
                    if (eEtat == EEtatName.init || eEtat == EEtatName.postComma)
                    {
                        lCurrent.Clear();
                        lLastName.Clear();
                        lFirstName.Clear();
                    }
                    continue;
                }
                if (s.IndexOf(",") >= 0 && eEtat == EEtatName.init)
                {
                    eEtat = EEtatName.postComma;
                    lLastName = lCurrent;
                    lCurrent = new List<string>();
                    continue;
                }
                if (eEtat == EEtatName.inDate)
                {
                    lDatepict.Add(s);
                    continue;
                }
                if (int.TryParse(s, out _) || IsMonth(s))
                {
                    if (eEtat == EEtatName.init) ProcessFirstLastName(ref lCurrent, ref lLastName, ref lFirstName);
                    eEtat = EEtatName.inDate;
                    lDatepict.Add(s);
                    continue;
                }
                if (eEtat == EEtatName.inPlace)
                {
                    lPlace.Add(s);
                    continue;
                }
                if (IsPlace(s))
                {
                    if (eEtat == EEtatName.init) ProcessFirstLastName(ref lCurrent, ref lLastName, ref lFirstName);
                    eEtat = EEtatName.inPlace;
                    lPlace.Add(s);
                    continue;
                }
                if (eEtat == EEtatName.postComma)
                {
                    lFirstName.Add(s);
                    continue;
                }
                lCurrent.Add(s);
                //Console.WriteLine(s);
            }
            if (eEtat == EEtatName.init) ProcessFirstLastName(ref lCurrent, ref lLastName, ref lFirstName);
            MergeName(ref lastname, String.Join(" ", lLastName.ToArray()), "id=" + _id);
            MergeName(ref firstname, String.Join(" ", lFirstName.ToArray()), "id=" + _id);
            String sPlace = String.Join(" ", lPlace.ToArray());
            sPlace = sPlace.Replace(",", "").Replace(")", "").Trim();
            MergeName(ref place, sPlace, "id=" + _id);
            MergeName(ref datePict, String.Join(" ", lDatepict.ToArray()), "id=" + _id);
        }

        readonly static String[] MONTHS = new String[] { "janvier", "février", "fevrier", "mars", "avril", "mai", "juin", "juillet", "aout", "août", "septembre", "octobre", "novembre", "decembre", "décembre" };
        private static bool IsMonth(String s)
        {
            return MONTHS.Contains(s.ToLower());
        }
        readonly static String[] TITLES = new String[] { "m", "mr", "mme", "me", "dr", "princesse", "sir", "professeur", "frère", "sa", "sainteté", "le", "pape", "colonel", "général", "miss", "mrs", "monseigneur", "melle" };
        private static bool IsTitle(String s)
        {
            return TITLES.Contains(s.ToLower());
        }
        readonly static String[] PLACES = new String[] { "unesco", "paris", "new-york", "abu", "simbel", "egypte", "monte", "montevideo" };
        private static bool IsPlace(String s)
        {
            return PLACES.Contains(s.ToLower());
        }
        private static void ProcessFirstLastName(ref List<String> lCurrent, ref List<String> lLastName, ref List<String> lFirstName)
        {
            if (lCurrent.Count == 0) return;
            if (lCurrent.Count == 1)
            {
                lLastName.Add(lCurrent.First());
            }
            else
            {
                lFirstName.Add(lCurrent.First());
                lCurrent.RemoveAt(0);
                lLastName = lCurrent;
            }
            lCurrent = new List<string>();
        }

        /*
        void GetPlace(String sIn)
        {
            
        }
        */

        static void MergeName(ref String dest, String from, String msg)
        {
            if (from is null) return;
            if (dest is null) { dest = from; return; }
            if (dest.Contains(from)) return;
            if (from.Contains(dest)) { dest = from; return; }
            Console.WriteLine(msg + " from={" + from + "} dest={" + dest + "}");
        }

        public void DoProcess()
        {
            _dateDone = DateTime.Now;
            UpdateDateDone();
        }

        public String AvoidWarning()
        {
            return ROOTTARGET + _id.ToString() + _doubleOf.ToString();
        }

        public CPortrait(System.IO.FileInfo oFile)
        {
            _folder = oFile.DirectoryName;
            _filename = oFile.Name;
            _lg = oFile.Length;
            var fileRadix = Path.GetFileNameWithoutExtension(oFile.Name);

            var oMD5 = System.Security.Cryptography.MD5.Create();
            oMD5.ComputeHash(oFile.OpenRead());
            _MD5 = BitConverter.ToString(oMD5.Hash).Replace("-", "");
            //Console.WriteLine("MD5=" + _MD5);

            // prefer a number followed by a "("
            var m = System.Text.RegularExpressions.Regex.Match(fileRadix, @"^([^\d]*)(\d+)\s*(\(.*)$");
            if (!m.Success)
            {
                m = System.Text.RegularExpressions.Regex.Match(fileRadix, @"^(.*\D)(\d+)\s*(\(.*)$");
                if (!m.Success)
                {
                    m = System.Text.RegularExpressions.Regex.Match(fileRadix, @"^([^\d]*)(\d+)(.*)$");
# if TRACE_ANA_ID
                    if (m.Success) Console.WriteLine("succeed reg3 " + m.Groups[1].Value + " " + m.Groups[2].Value + "  " + m.Groups[3].Value);
#endif
                }
#if TRACE_ANA_ID
                else Console.WriteLine("succeed reg2 " + m.Groups[1].Value + " " + m.Groups[2].Value + "  " + m.Groups[3].Value);
#endif
            }
# if TRACE_ANA_ID
            else Console.WriteLine("succeed reg1 " + m.Groups[1].Value + " " + m.Groups[2].Value + "  " + m.Groups[3].Value);
#endif
            if (!m.Success)
            {
                _id1 = 0;
                _id2 = 0;
                _textAfter = fileRadix.Trim();
                Console.WriteLine("No ID " + oFile.FullName + "   ");
                return;
            }

            _id1 = int.Parse(m.Groups[2].Value);

            String sBefore = m.Groups[1].Value.Trim();
            String sAfter = m.Groups[3].Value.Trim();
            if (!String.IsNullOrEmpty(sBefore)) _textBefore = sBefore;
            if (!String.IsNullOrEmpty(sAfter))
            {
                m = System.Text.RegularExpressions.Regex.Match(sAfter, @"^\(\s*(\d*)\s*\)(.*)$");
                if (!m.Success)
                {
                    _textAfter = sAfter;
#if TRACE_ANA_ID
                    Console.WriteLine("failed reg id2");
#endif
                }
                else
                {
                    int.TryParse(m.Groups[1].Value, out _id2);
                    sAfter = m.Groups[2].Value.Trim();
                    if (!String.IsNullOrEmpty(sAfter)) _textAfter = sAfter;
#if TRACE_ANA_ID
                    Console.WriteLine("suceed reg id2 " + m.Groups[1].Value + " " + m.Groups[2].Value + "  ");
#endif
                }
            }
            String sql = "select id from PortraitNASList where MD5=@md5 order by id";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                cmd.Parameters.AddWithValue("@md5", _MD5);
                using (var rs = cmd.ExecuteReader())
                {
                    if (rs.Read()) _doubleOf = rs.GetInt32(0);
                }
            }
            Console.Write(_id1.ToString() + " " + _id2 + " " + oFile.FullName.Replace(ROOTSOURCE, "") + "    \r");
        }

        public CPortrait(System.Data.SqlClient.SqlDataReader rs)
        {
            _id = rs.GetInt32((int)ERs.id);
            _id1 = rs.GetInt32((int)ERs.id1);
            _id2 = rs.GetInt32((int)ERs.id2);
            _id3 = rs.GetInt32((int)ERs.id3);
            _folder = rs.GetString((int)ERs.Folder);
            _filename = rs.GetString((int)ERs.Filename);
            if (!rs.IsDBNull((int)ERs.textBefore)) _textBefore = rs.GetString((int)ERs.textBefore);
            if (!rs.IsDBNull((int)ERs.textAfter)) _textAfter = rs.GetString((int)ERs.textAfter);
            _lg = rs.GetInt64((int)ERs.SizeJPG);
            if (!rs.IsDBNull((int)ERs.dateDone)) _dateDone = rs.GetDateTime((int)ERs.dateDone);
            _MD5 = rs.GetString((int)ERs.MD5);
            if (!rs.IsDBNull((int)ERs.doubleOf)) _doubleOf = rs.GetInt32((int)ERs.doubleOf);
            if (!rs.IsDBNull((int)ERs.lastname)) lastname = rs.GetString((int)ERs.lastname);
            if (!rs.IsDBNull((int)ERs.firstname)) firstname = rs.GetString((int)ERs.firstname);
            if (!rs.IsDBNull((int)ERs.place)) place = rs.GetString((int)ERs.place);
            if (!rs.IsDBNull((int)ERs.datepict)) datePict = rs.GetString((int)ERs.datepict);
            seqnoName = 0;
            if (!rs.IsDBNull((int)ERs.seqnoName)) seqnoName = rs.GetInt32((int)ERs.seqnoName);
        }

        public String MD5
        {
            get { return _MD5; }
        }

        public int ID
        {
            get { return _id; }
        }

        public void UpdateDateDone()
        {
            String sql = "update PortraitNASList set doneDate=@d where id1=@id1 and id2=@id2 and id3=@id3";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                cmd.Parameters.AddWithValue("@d", _dateDone);
                cmd.Parameters.AddWithValue("@id1", _id1);
                cmd.Parameters.AddWithValue("@id2", _id2);
                cmd.Parameters.AddWithValue("@id3", _id3);
                cmd.ExecuteNonQuery();
            }
        }

        /*
        public int? CheckDouble()
        {
            String sql = "select id from PortraitNASList where MD5=@md5 order by id";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                cmd.Parameters.AddWithValue("@md5", _MD5);
                using (var rs = cmd.ExecuteReader())
                {
                    if (!rs.Read()) return null;
                    _doubleOf = rs.GetInt32(0);
                    return _doubleOf;
                }
            }
        }
        */

        public void SQLInsert()
        {
            String sql = "select max(id3) from PortraitNASList where id1=@id1 and id2=@id2";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                cmd.Parameters.AddWithValue("@id1", _id1);
                cmd.Parameters.AddWithValue("@id2", _id2);
                var oRet = cmd.ExecuteScalar();
                if (oRet == null || oRet == DBNull.Value) _id3 = 1;
                else _id3 = ((int)oRet) + 1;
            }
            sql = @"insert into PortraitNASList(" + FIELD_LIST.Replace(", DoubleOf, id", "") + @") values 
                    (@id1, @id2, @id3, @folder, @file, @tb, @ta, @lg, null, @md5)";
            using (var cmd = new System.Data.SqlClient.SqlCommand(sql, Program.conn))
            {
                cmd.Parameters.AddWithValue("@id1", _id1);
                cmd.Parameters.AddWithValue("@id2", _id2);
                cmd.Parameters.AddWithValue("@id3", _id3);
                cmd.Parameters.AddWithValue("@folder", _folder);
                cmd.Parameters.AddWithValue("@file", _filename);
                if (String.IsNullOrEmpty(_textBefore)) cmd.Parameters.AddWithValue("@tb", DBNull.Value);
                else cmd.Parameters.AddWithValue("@tb", _textBefore);
                if (String.IsNullOrEmpty(_textAfter)) cmd.Parameters.AddWithValue("@ta", DBNull.Value);
                else cmd.Parameters.AddWithValue("@ta", _textAfter);
                cmd.Parameters.AddWithValue("@lg", _lg);
                cmd.Parameters.AddWithValue("@md5", _MD5);
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }

        public override String ToString()
        {
            return _id1.ToString() + " " + _id2.ToString() + " " + _id3.ToString() + " " + _textBefore + "/" + _textAfter;
        }
    }
}
