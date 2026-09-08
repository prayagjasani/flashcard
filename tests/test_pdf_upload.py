import json
import unittest
from io import BytesIO
from unittest.mock import MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image
from routers import pdfs


class PdfUploadTest(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(pdfs.router)
        self.client = TestClient(app)
        self.storage = MagicMock()
        self.storage.get_object.side_effect = lambda **_: {'Body': BytesIO(b'[]')}

    def test_upload_accepts_unicode_title_and_preserves_folder(self):
        with patch.object(pdfs, 'r2_client', self.storage), patch.object(pdfs, 'R2_BUCKET_NAME', 'test'), patch.object(pdfs, '_build_thumb', return_value=None):
            response = self.client.post('/pdf/upload', data={'name': 'Folgen ausdrücken', 'folder': 'A1'}, files={'file': ('document.pdf', b'%PDF-test', 'application/pdf')})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['name'], pdfs._safe_name('Folgen ausdrücken'))
        index = json.loads(self.storage.put_object.call_args.kwargs['Body'])
        self.assertEqual(index[0]['folder'], 'A1')

    def test_upload_rejects_oversize_before_render_or_storage(self):
        with patch.object(pdfs, 'r2_client', self.storage), patch.object(pdfs, 'R2_BUCKET_NAME', 'test'), patch.object(pdfs, 'PDF_UPLOAD_MAX_BYTES', 10), patch.object(pdfs, '_build_thumb') as render:
            response = self.client.post('/pdf/upload', data={'name': 'Large'}, files={'file': ('large.pdf', b'x' * 11, 'application/pdf')})
        self.assertEqual(response.status_code, 413)
        render.assert_not_called()
        self.storage.put_object.assert_not_called()

    @unittest.skipIf(pdfs.pdfium is None, 'PDFium unavailable')
    def test_real_thumbnail_render_closes_resources_and_bounds_image(self):
        source = BytesIO()
        with Image.new('RGB', (1600, 2200), 'white') as image:
            image.save(source, format='PDF')
        with patch.object(pdfs, 'r2_client', self.storage), patch.object(pdfs, 'R2_BUCKET_NAME', 'test'):
            for _ in range(3):
                self.assertIsNotNone(pdfs._build_thumb(source.getvalue(), 'Preview'))
                with Image.open(BytesIO(self.storage.put_object.call_args.kwargs['Body'])) as result:
                    self.assertLessEqual(max(result.size), 1024)
