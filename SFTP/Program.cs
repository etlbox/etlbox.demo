using ETLBox.DataFlow;
using Renci.SshNet;

//Initialization
SftpClient client = new SftpClient("test.rebex.net", "demo", "password");
client.Connect();

//We use a simple text file as source
TextSource<FileData> source = new TextSource<FileData>("readme.txt");
source.CreateStreamReader = (file) => {
    return new StreamReader(client.OpenRead(file));
};

//For each line we create a FileData object that contains the current line and the row number
source.ParseLineFunc = (line,progress) => {
    return new FileData() {
        Line = line,
        RowNumber = progress
    };
};

//We use a custom destination to print out the line and the row number on the console
CustomDestination<FileData> dest = new CustomDestination<FileData>();
dest.WriteAction = (data, pc) => {
    Console.WriteLine($"Line {data.RowNumber}: {data.Line}");
};

//Link source and destination together
source.LinkTo(dest);

//Execute the data flow
Network.Execute(source);

//Cleanup
client.Disconnect();

//Class Definition
public class FileData {
    public string Line { get; set; }
    public int RowNumber { get; set; }
}