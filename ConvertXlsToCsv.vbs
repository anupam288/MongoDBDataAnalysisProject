if WScript.Arguments.Count < 2 Then
    WScript.Echo "Please specify the source and the destination files. Usage: ExcelToCsv <xls/xlsx source file> <csv destination file>"
    Wscript.Quit
End If

csv_format = 6

Set objFSO = CreateObject("Scripting.FileSystemObject")

src_file = objFSO.GetAbsolutePathName(Wscript.Arguments.Item(0))

WScript.Echo src_file
 
dest_file = objFSO.GetAbsolutePathName(WScript.Arguments.Item(1))

WScript.Echo dest_file

Dim oExcel
Set oExcel = CreateObject("Excel.Application")

Dim oBook
Set oBook = oExcel.Workbooks.Open(src_file)

oBook.SaveAs dest_file, csv_format

oBook.Close False
oExcel.Quit