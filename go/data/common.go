// This module contains Flyte CoPilot related code.
// Currently it only has 2 utilities - downloader and an uploader.
// Usage Downloader:
//  downloader := NewDownloader(...)
//  downloader.DownloadInputs(...) // will recursively download all inputs
//
// Usage uploader:
// uploader := NewUploader(...)
// uploader.RecursiveUpload(...) // Will recursively upload all the data from the given path depending on the output interface
// All errors are bubbled up.
//
// Both the uploader and downloader accept context.Context variables. These should be used to control timeouts etc.
// TODO: Currently retries are not automatically handled.
package data

type Format = string

const (
	FormatJSON  Format = "json"
	FormatYAML  Format = "yaml"
	FormatProto Format = "proto"
)

var AllOutputFormats = []Format{
	FormatJSON,
	FormatYAML,
	FormatProto,
}

type VarMap map[string]interface{}
type FutureMap map[string]Future
