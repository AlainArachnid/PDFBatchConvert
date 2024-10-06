using System;
using System.Collections.Generic;
using iText.Kernel.Pdf;
using iText.Kernel.Pdf.Canvas;
using iText.Kernel.Pdf.Canvas.Parser;
using iText.Kernel.Pdf.Canvas.Parser.Listener;
using iText.Kernel.Pdf.Canvas.Parser.Data;

namespace UnesdocBatchConvert
{
    // code from https://stackoverflow.com/questions/40951776/manipulate-paths-color-etc-in-itext/40999180#40999180
    public abstract class PdfCanvasEditor2 : PdfCanvasProcessor
    {

        //
        // constructor giving the parent a dummy listener to talk to 
        //
        public PdfCanvasEditor2() : base(new DummyEventListener())
        {
        }

        /**
         * This method edits the immediate contents of a page, i.e. its content stream.
         * It explicitly does not descent into form xobjects, patterns, or annotations.
         */
        public void EditPage(iText.Kernel.Pdf.PdfDocument oDocumentIn, int pageNumberIn, iText.Kernel.Pdf.PdfDocument oDocumentOut, int pageNumberOut)
        {
            if (oDocumentIn.GetReader() == null)
            {
                throw new iText.Kernel.Exceptions.PdfException("PdfDocumentIn must have reader.");
            }
            if (oDocumentOut.GetWriter() == null)
            {
                throw new iText.Kernel.Exceptions.PdfException("PdfDocumentOut must have writer.");
            }

            var pageIn = oDocumentIn.GetPage(pageNumberIn);
            var pageOut = oDocumentOut.GetPage(pageNumberOut);
            PreEditPage(pageIn, pageOut, oDocumentIn, pageNumberIn, oDocumentOut, pageNumberOut);
            var oResources = pageIn.GetResources();
            var oCanvas = new iText.Kernel.Pdf.Canvas.PdfCanvas(new iText.Kernel.Pdf.PdfStream(), oResources, oDocumentOut);
            EditContent(pageIn.GetContentBytes(), oResources, oCanvas);
            pageOut.Put(PdfName.Contents, oCanvas.GetContentStream());
            PostEditPage(pageIn, pageOut, oDocumentIn, pageNumberIn, oDocumentOut, pageNumberOut);
        }

        /** allow child class to do more
         */
        public virtual void PreEditPage(PdfPage pageIn, PdfPage pageOut, PdfDocument oDocumentIn, int pageNumberIn, PdfDocument oDocumentOut, int pageNumberOut)
        {
        }

        public virtual void PostEditPage(PdfPage pageIn, PdfPage pageOut, PdfDocument oDocumentIn, int pageNumberIn, PdfDocument oDocumentOut, int pageNumberOut)
        {
        }

        /**
         * This method processes the content bytes and outputs to the given canvas.
         * It explicitly does not descent into form xobjects, patterns, or annotations.
         */
        public void EditContent(byte[] contentBytes, PdfResources resources, PdfCanvas canvas)
        {
            this.canvas = canvas;
            ProcessContent(contentBytes, resources);
            this.canvas = null;
        }

        /**
         * <p>
         * This method writes content stream operations to the target canvas. The default
         * implementation writes them as they come, so it essentially generates identical
         * copies of the original instructions the {@link ContentOperatorWrapper} instances
         * forward to it.
         * </p>
         * <p>
         * Override this method to achieve some fancy editing effect.
         * </p> 
         */
        protected virtual void Write(PdfCanvasProcessor processor, PdfLiteral oOperator, IList<PdfObject> operands)
        {
            if (processor is null)
            {
                throw new ArgumentNullException(nameof(processor));
            }

            if (oOperator is null)
            {
                throw new ArgumentNullException(nameof(oOperator));
            }

            if (operands is null)
            {
                throw new ArgumentNullException(nameof(operands));
            }

            PdfOutputStream oOutputStream = canvas.GetContentStream().GetOutputStream();
            int index = 0;

            foreach (PdfObject oObject in operands)
            {
                oOutputStream.Write(oObject);
                if (operands.Count > ++index)
                    oOutputStream.WriteSpace();
                else
                    oOutputStream.WriteNewLine();
            }
        }


        //
        // Overrides of PdfContentStreamProcessor methods
        //
        public override IContentOperator RegisterContentOperator(String operatorString, IContentOperator oOperator)
        {
            ContentOperatorWrapper wrapper = new ContentOperatorWrapper(this);
            wrapper.SetOriginalOperator(oOperator);
            IContentOperator formerOperator = base.RegisterContentOperator(operatorString, wrapper);
            return formerOperator is ContentOperatorWrapper wrapper1 ? wrapper1.GetOriginalOperator() : formerOperator;
        }

        //
        // members holding the output canvas and the resources
        //
        protected PdfCanvas canvas = null;

        //
        // A content operator class to wrap all content operators to forward the invocation to the editor
        //
        class ContentOperatorWrapper : IContentOperator
        {
            readonly PdfCanvasEditor2 oParent;

            public ContentOperatorWrapper(PdfCanvasEditor2 paramParent)
            {
                oParent = paramParent;
            }

            public IContentOperator GetOriginalOperator()
            {
                return originalOperator;
            }

            public void SetOriginalOperator(IContentOperator originalOperator)
            {
                this.originalOperator = originalOperator;
            }

            void IContentOperator.Invoke(PdfCanvasProcessor processor, PdfLiteral @oOperator, IList<PdfObject> lOperands)
            {
                if (originalOperator != null && "Do" != oOperator.ToString())
                {
                    originalOperator.Invoke(processor, oOperator, lOperands);
                }
                oParent.Write(processor, oOperator, lOperands);
            }

            private IContentOperator originalOperator = null;
        }

        //
        // A dummy event listener to give to the underlying canvas processor to feed events to
        //
        class DummyEventListener : IEventListener
        {
            void IEventListener.EventOccurred(IEventData data, EventType type)
            {
            }

            ICollection<EventType> IEventListener.GetSupportedEvents()
            {
                return null;
            }
        }
    }
    public abstract class PdfCanvasEditor : PdfCanvasProcessor
    {

        //
        // constructor giving the parent a dummy listener to talk to 
        //
        public PdfCanvasEditor() : base(new DummyEventListener())
        {
        }

        /**
         * This method edits the immediate contents of a page, i.e. its content stream.
         * It explicitly does not descent into form xobjects, patterns, or annotations.
         */
        public void EditPage(iText.Kernel.Pdf.PdfDocument oDocument, int pageNumber)
        {
            if ((oDocument.GetReader() == null) || (oDocument.GetWriter() == null))
            {
                //throw new iText.Kernel.Exceptions.PdfException("PdfDocument must be opened in stamping mode.");
            }

            var page = oDocument.GetPage(pageNumber);
            PreEditPage(page, oDocument, pageNumber);
            var oResources = page.GetResources();
            var oCanvas = new iText.Kernel.Pdf.Canvas.PdfCanvas(new iText.Kernel.Pdf.PdfStream(), oResources, oDocument);
            EditContent(page.GetContentBytes(), oResources, oCanvas);
            page.Put(PdfName.Contents, oCanvas.GetContentStream());
            PostEditPage(page, oDocument, pageNumber);
        }

        /** allows child class to do more
         */
        public virtual void PreEditPage(PdfPage page, PdfDocument oDocument, int pageNumber)
        {
        }

        public virtual void PostEditPage(PdfPage page, PdfDocument oDocument, int pageNumber)
        {
        }

        /**
         * This method processes the content bytes and outputs to the given canvas.
         * It explicitly does not descent into form xobjects, patterns, or annotations.
         */
        public void EditContent(byte[] contentBytes, PdfResources resources, PdfCanvas canvas)
        {
            this.canvas = canvas;
            ProcessContent(contentBytes, resources);
            this.canvas = null;
        }

        /**
         * <p>
         * This method writes content stream operations to the target canvas. The default
         * implementation writes them as they come, so it essentially generates identical
         * copies of the original instructions the {@link ContentOperatorWrapper} instances
         * forward to it.
         * </p>
         * <p>
         * Override this method to achieve some fancy editing effect.
         * </p> 
         */
        protected virtual void Write(PdfCanvasProcessor processor, PdfLiteral oOperator, IList<PdfObject> operands)
        {
            if (processor is null)
            {
                throw new ArgumentNullException(nameof(processor));
            }

            if (oOperator is null)
            {
                throw new ArgumentNullException(nameof(oOperator));
            }

            if (operands is null)
            {
                throw new ArgumentNullException(nameof(operands));
            }

            PdfOutputStream oOutputStream = canvas.GetContentStream().GetOutputStream();
            int index = 0;

            foreach (PdfObject oObject in operands)
            {
                oOutputStream.Write(oObject);
                if (operands.Count > ++index)
                    oOutputStream.WriteSpace();
                else
                    oOutputStream.WriteNewLine();
            }
        }


        //
        // Overrides of PdfContentStreamProcessor methods
        //
        public override IContentOperator RegisterContentOperator(String operatorString, IContentOperator oOperator)
        {
            ContentOperatorWrapper wrapper = new ContentOperatorWrapper(this);
            wrapper.SetOriginalOperator(oOperator);
            IContentOperator formerOperator = base.RegisterContentOperator(operatorString, wrapper);
            return formerOperator is ContentOperatorWrapper wrapper1 ? wrapper1.GetOriginalOperator() : formerOperator;
        }

        //
        // members holding the output canvas and the resources
        //
        protected PdfCanvas canvas = null;

        //
        // A content operator class to wrap all content operators to forward the invocation to the editor
        //
        class ContentOperatorWrapper : IContentOperator
        {
            readonly PdfCanvasEditor oParent;

            public ContentOperatorWrapper(PdfCanvasEditor paramParent)
            {
                oParent = paramParent;
            }

            public IContentOperator GetOriginalOperator()
            {
                return originalOperator;
            }

            public void SetOriginalOperator(IContentOperator originalOperator)
            {
                this.originalOperator = originalOperator;
            }

            void IContentOperator.Invoke(PdfCanvasProcessor processor, PdfLiteral @oOperator, IList<PdfObject> lOperands)
            {
                if (originalOperator != null && "Do" != oOperator.ToString())
                {
                    originalOperator.Invoke(processor, oOperator, lOperands);
                }
                oParent.Write(processor, oOperator, lOperands);
            }

            private IContentOperator originalOperator = null;
        }

        //
        // A dummy event listener to give to the underlying canvas processor to feed events to
        //
        class DummyEventListener : IEventListener
        {
            void IEventListener.EventOccurred(IEventData data, EventType type)
            {
            }

            ICollection<EventType> IEventListener.GetSupportedEvents()
            {
                return null;
            }
        }
    }
}
