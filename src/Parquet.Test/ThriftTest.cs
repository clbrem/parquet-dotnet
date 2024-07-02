using Xunit;
using Parquet.Meta.Proto;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Parquet.Meta;

namespace Parquet.Test {
    public class ThriftTest : TestBase {
        [Fact]
        public void TestFileRead_Table() {
            using Stream fs = OpenTestFile("thrift/wide.bin");
            var fileMeta = FileMetaData.Read(new ThriftCompactProtocolReader(fs));
        }
        [Fact]
        public async Task TestFile() {
            await using Stream fs = new MemoryStream();
            byte[] buff = new int[] { 25, 76, 37, 2, 0, 37, 4, 0, 37, 6, 0, 37, 8, 0, 0}.Select(a => (byte)a).ToArray();
            await fs.WriteAsync(buff);
            fs.Position = 0;
            var reader = new ThriftCompactProtocolReader(fs);
            reader.StructBegin();
            reader.ReadNextField(out short fieldId, out CompactType ct);
            Assert.Equal(1, fieldId);
            Assert.Equal(CompactType.List, ct);
            reader.SkipField(CompactType.List);
            
        }
    }
}