#!/usr/bin/env python3
import os
import logging
from io import BytesIO
from minio import Minio
from minio.error import S3Error
from telegram import Update, InputFile
from telegram.ext import Updater, CommandHandler MessgeHandler, Filters, CallbackContext

# logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# config desde env
TOKEN = os.getenv('TELEGRAM_TOKEN')
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
BUCKET = os.getenv('MINIO_BUCKET', 'drive')

# cliente MinIO
client = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # dentro de la red docker no hace falta TLS
)

# asegurar bucket
if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)

# handlers
def start(update: Update, context: CallbackContext):
    update.message.reply_text(
        'Hola — soy tu Drive personal. '
        'Manda archivos para subir o usa /list /get <name> /share <name>.'
    )

def upload_file(update: Update, context: CallbackContext):
    if update.message.document:
        doc = update.message.document
        file = doc.get_file()
        bio = BytesIO()
        file.download(out=bio)
        bio.seek(0)
        object_name = doc.file_name
        client.put_object(
            BUCKET,
            object_name,
            data=bio,
            length=bio.getbuffer().nbytes
        )
        update.message.reply_text(f'Archivo subido: {object_name}')
    else:
        update.message.reply_text('Envía un archivo (documento) para subirlo.')

def list_files(update: Update, context: CallbackContext):
    objects = client.list_objects(BUCKET, recursive=True)
    names = [obj.object_name for obj in objects]
    if not names:
        update.message.reply_text('Bucket vacío.')
    else:
        reply = "\n".join(names[:50])
        update.message.reply_text('Archivos:\n' + reply)

def get_file(update: Update, context: CallbackContext):
    args = context.args
    if not args:
        update.message.reply_text('Uso: /get <nombre_de_archivo>')
        return
    name = args[0]
    try:
        response = client.get_object(BUCKET, name)
        bio = BytesIO(response.read())
        bio.seek(0)
        update.message.reply_document(document=InputFile(bio, filename=name))
    except S3Error as e:
        update.message.reply_text(f'Error: {e}')

def share_file(update: Update, context: CallbackContext):
    args = context.args
    if not args:
        update.message.reply_text('Uso: /share <nombre>')
        return
    name = args[0]
    # generar URL pre-firmada (1 hora)
    try:
        url = client.presigned_get_object(BUCKET, name, expires=3600)
        update.message.reply_text(f'URL (1h): {url}')
    except Exception as e:
        update.message.reply_text(f'Error: {e}')

def main():
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler('start', start))
    dp.add_handler(CommandHandler('list', list_files))
    dp.add_handler(CommandHandler('get', get_file))
    dp.add_handler(CommandHandler('share', share_file))
    dp.add_handler(MessageHandler(Filters.document, upload_file))

    updater.start_polling()
    updater.idle()

if __name__ == '__main__':
    main()
