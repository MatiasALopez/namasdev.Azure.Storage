using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

using namasdev.Core.IO;
using namasdev.Core.Validation;

namespace namasdev.Azure.Storage
{
    public class ArchivosRepositorio
    {
        private CloudStorageAccount _account;

        public ArchivosRepositorio(CloudStorageAccount account)
        {
            Validador.ValidarArgumentRequeridoYThrow(account, nameof(account));

            _account = account;
        }

        private CloudBlobClient _blobClient;
        protected CloudBlobClient BlobClient
        {
            get { return _blobClient ?? (_blobClient = _account.CreateCloudBlobClient()); }
        }

        public string ObtenerUrlArchivo(string contenedor, string nombreArchivo,
            params string[] directorios)
        {
            var blob = ObtenerReferenciaBlob(contenedor, nombreArchivo, directorios);
            return blob.Uri.AbsoluteUri;
        }

        public static string ObtenerNombreDirectorio(string url)
        {
            if (String.IsNullOrWhiteSpace(url))
            {
                return url;
            }

            var partes = Path.GetDirectoryName(new Uri(url).PathAndQuery)
                .Substring(1)
                .Split('\\');

            return partes.Length > 1
                ? String.Join("/", partes.Skip(1))
                : String.Empty;
        }

        public async Task<string> AgregarAsync(string contenedor, Archivo archivo, params string[] directorios)
        {
            Validador.ValidarArgumentRequeridoYThrow(archivo, nameof(archivo));

            var blob = ObtenerReferenciaBlob(contenedor, archivo.Nombre, directorios);
            await blob.UploadFromByteArrayAsync(archivo.Contenido, 0, archivo.Contenido.Length);

            return blob.Uri.AbsoluteUri;
        }

        public string Agregar(string contenedor, Archivo archivo, params string[] directorios)
        {
            Validador.ValidarArgumentRequeridoYThrow(archivo, nameof(archivo));

            var blob = ObtenerReferenciaBlob(contenedor, archivo.Nombre, directorios);
            blob.UploadFromByteArray(archivo.Contenido, 0, archivo.Contenido.Length);

            return blob.Uri.AbsoluteUri;
        }

        public Archivo ObtenerArchivo(string contenedor, string nombreArchivo,
            params string[] directorios)
        {
            var blob = ObtenerReferenciaBlob(contenedor, nombreArchivo, directorios);
            return new Archivo
            {
                Nombre = Path.GetFileName(nombreArchivo),
                Contenido = ObtenerBytes(blob)
            };
        }

        public async Task<byte[]> ObtenerBytesAsync(string url)
        {
            return await ObtenerBytesAsync(new Uri(url));
        }

        public async Task<byte[]> ObtenerBytesAsync(Uri uri)
        {
            var blob = BlobClient.GetBlobReferenceFromServer(uri);

            blob.FetchAttributes();
            var bytes = new byte[blob.Properties.Length];
            await blob.DownloadToByteArrayAsync(bytes, 0);

            return bytes;
        }

        public byte[] ObtenerBytes(string url)
        {
            return ObtenerBytes(new Uri(url));
        }

        public byte[] ObtenerBytes(Uri uri)
        {
            var blob = BlobClient.GetBlobReferenceFromServer(uri);

            blob.FetchAttributes();
            var bytes = new byte[blob.Properties.Length];
            blob.DownloadToByteArray(bytes, 0);

            return bytes;
        }

        private byte[] ObtenerBytes(ICloudBlob blob)
        {
            blob.FetchAttributes();
            var bytes = new byte[blob.Properties.Length];
            blob.DownloadToByteArray(bytes, 0);

            return bytes;
        }

        private CloudBlockBlob ObtenerReferenciaBlob(
            string contenedor, string nombreArchivo,
            params string[] directorios)
        {
            if (directorios == null || !directorios.Any())
            {
                return ObtenerContainer(contenedor)
                    .GetBlockBlobReference(nombreArchivo);
            }
            else
            {
                return ObtenerDirectorio(contenedor, directorios)
                    .GetBlockBlobReference(nombreArchivo);
            }
        }

        public IEnumerable<IListBlobItem> ListarBlobs(string contenedor,
            params string[] directorios)
        {
            if (directorios == null || !directorios.Any())
            {
                return ObtenerContainer(contenedor)
                    .ListBlobs(useFlatBlobListing: true);
            }
            else
            {
                return ObtenerDirectorio(contenedor, directorios)
                    .ListBlobs(useFlatBlobListing: true);
            }
        }

        public async Task<string> ObtenerStringAsync(Uri uri)
        {
            var bytes = await ObtenerBytesAsync(uri);
            return Encoding.UTF8.GetString(bytes);
        }

        public void Guardar(string url, string archivoPath)
        {
            Guardar(new Uri(url), archivoPath);
        }

        public void Guardar(Uri uri, string archivoPath)
        {
            var blob = BlobClient.GetBlobReferenceFromServer(uri);

            blob.DownloadToFile(archivoPath, FileMode.CreateNew);
        }

        public async Task CopiarBlobAsync(Uri blobUri, string nombreContenedorDestino,
            params string[] directorios)
        {
            var blobOrigen = BlobClient.GetBlobReferenceFromServer(blobUri);
            var blobOrigenBytes = ObtenerBytes(blobOrigen);

            var blobDestino = ObtenerReferenciaBlob(nombreContenedorDestino, Path.GetFileName(blobOrigen.Name), directorios);
            await blobDestino.UploadFromByteArrayAsync(blobOrigenBytes, 0, blobOrigenBytes.Length);
        }

        public async Task MoverBlobAsync(Uri blobUri, string nombreContenedorDestino,
            params string[] directorios)
        {
            await CopiarBlobAsync(blobUri, nombreContenedorDestino, directorios);
            await EliminarAsync(blobUri);
        }

        public async Task EliminarAsync(Uri blobUri)
        {
            var blob = BlobClient.GetBlobReferenceFromServer(blobUri);
            await blob.DeleteIfExistsAsync();
        }

        public async Task EliminarAsync(string contenedor, string nombreArchivo, params string[] directorios)
        {
            var blob = ObtenerReferenciaBlob(contenedor, nombreArchivo, directorios);
            await blob.DeleteIfExistsAsync();
        }

        public void Eliminar(Uri blobUri)
        {
            var blob = BlobClient.GetBlobReferenceFromServer(blobUri);
            blob.DeleteIfExists();
        }

        public void Eliminar(string contenedor, string nombreArchivo, params string[] directorios)
        {
            var blob = ObtenerReferenciaBlob(contenedor, nombreArchivo, directorios);
            blob.DeleteIfExists();
        }

        private CloudBlobContainer ObtenerContainer(string contenedor)
        {
            return BlobClient.GetContainerReference(contenedor);
        }

        private CloudBlobDirectory ObtenerDirectorio(string contenedor, string[] directorios)
        {
            Validador.ValidarArgumentRequeridoYThrow(directorios, nameof(directorios));

            return BlobClient
                .GetContainerReference(contenedor)
                .GetDirectoryReference(String.Join("/", directorios));
        }
    }
}
