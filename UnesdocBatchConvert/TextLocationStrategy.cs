using iText.Kernel.Geom;
using iText.Kernel.Pdf.Canvas.Parser;
using iText.Kernel.Pdf.Canvas.Parser.Data;
using iText.Kernel.Pdf.Canvas.Parser.Listener;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace UnesdocBatchConvert
{
    class TextLocationStrategy : LocationTextExtractionStrategy
    {
        private readonly List<TextChunk> objectResult = new List<TextChunk>();

        public override void EventOccurred(IEventData data, EventType type)
        {
            if (!type.Equals(EventType.RENDER_TEXT))
                return;

            TextRenderInfo renderInfo = (TextRenderInfo)data;

            string curFont = renderInfo.GetFont().GetFontProgram().ToString();

            float curFontSize = renderInfo.GetFontSize();

            string letter = renderInfo.GetText();
            Vector letterStart = renderInfo.GetBaseline().GetStartPoint();
            Vector letterEnd = renderInfo.GetAscentLine().GetEndPoint();
            Rectangle letterRect = new Rectangle(letterStart.Get(0), letterStart.Get(1), letterEnd.Get(0) - letterStart.Get(0), letterEnd.Get(1) - letterStart.Get(1));
            //Console.WriteLine("==" + letter);

            TextChunk chunk = new TextChunk
            {
                Text = letter,
                Rect = letterRect,
                FontFamily = curFont,
                FontSize = (int)curFontSize,
                //SpaceWidth = renderInfo.GetSingleSpaceWidth() / 2f
            };

            objectResult.Add(chunk);
        }

        public List<TextChunk> GetResult()
        {
            return objectResult;
        }
    }
    public class TextChunk
    {
        public string Text { get; set; }
        public Rectangle Rect { get; set; }
        public string FontFamily { get; set; }
        public int FontSize { get; set; }
        //public float SpaceWidth { get; set; }
    }
}