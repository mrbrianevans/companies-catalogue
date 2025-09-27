
if (Test-Path bin){
    Remove-Item -Recurse bin
}

mkdir bin

Copy-Item crawler/crawl_sftp.py bin/

bun build metadataSummary/summarise.ts --target=bun --outfile bin/metadataSummary.ts

Set-Location saveFiles
$env:CGO_ENABLED=0;
$env:GOOS='linux';
#$env:GOOS = "windows"
$env:GOARCH='amd64';
go build -o ../bin/saveFiles
Set-Location ..