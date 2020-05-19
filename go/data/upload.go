package data

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/utils"
)

const maxPrimitiveSize = 1024

type Unmarshal func(r io.Reader, msg proto.Message) error
type Uploader struct {
	format    Format
	unmarshal Unmarshal
	// TODO support multiple buckets
	store                   *storage.DataStore
	aggregateOutputFileName string
	errorFileName           string
}

type dirFile struct {
	path string
	info os.FileInfo
	ref  storage.DataReference
}

func IsFileReadable(fpath string, ignoreExtension bool) (string, os.FileInfo, error) {
	info, err := os.Stat(fpath)
	if err != nil {
		if os.IsNotExist(err) {
			if ignoreExtension {
				logger.Infof(context.TODO(), "looking for any extensions")
				matches, err := filepath.Glob(fpath + ".*")
				if err == nil && len(matches) == 1 {
					logger.Infof(context.TODO(), "Extension match found [%s]", matches[0])
					info, err = os.Stat(matches[0])
					if err == nil {
						return matches[0], info, nil
					}
				} else {
					logger.Errorf(context.TODO(), "Extension match not found [%v,%v]", err, matches)
				}
			}
			return "", nil, errors.Wrapf(err, "file not found at path [%s]", fpath)
		}
		if os.IsPermission(err) {
			return "", nil, errors.Wrapf(err, "unable to read file [%s], Flyte does not have permissions", fpath)
		}
		return "", nil, errors.Wrapf(err, "failed to read file")
	}
	return fpath, info, nil
}

func (u Uploader) handleSimpleType(ctx context.Context, t core.SimpleType, filePath string) (*core.Literal, error) {
	if fpath, info, err := IsFileReadable(filePath, true); err != nil {
		return nil, err
	} else {
		if info.IsDir() {
			return nil, fmt.Errorf("expected file for type [%s], found dir at path [%s]", t.String(), filePath)
		}
		if info.Size() > maxPrimitiveSize {
			return nil, fmt.Errorf("maximum allowed filesize is [%d], but found [%d]", maxPrimitiveSize, info.Size())
		}
		filePath = fpath
	}
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return utils.MakeLiteralForSimpleType(t, string(b))
}

func UploadFile(ctx context.Context, filePath string, toPath storage.DataReference, size int64, store *storage.DataStore) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			logger.Errorf(ctx, "failed to close blob file at path [%s]", filePath)
		}
	}()
	return store.WriteRaw(ctx, toPath, size, storage.Options{}, f)
}

func (u Uploader) handleBlobType(ctx context.Context, localPath string, toPath storage.DataReference) (*core.Literal, error) {
	if fpath, info, err := IsFileReadable(localPath, true); err != nil {
		return nil, err
	} else {
		if info.IsDir() {
			var files []dirFile
			err := filepath.Walk(localPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					logger.Errorf(ctx, "encountered error when uploading multipart blob, %s", err)
					return err
				}
				if info.IsDir() {
					logger.Warnf(ctx, "Currently nested directories are not supported in multipart blob uploads, for directory @ %s", path)
				} else {
					ref, err := u.store.ConstructReference(ctx, toPath, info.Name())
					if err != nil {
						return err
					}
					files = append(files, dirFile{
						path: path,
						info: info,
						ref:  ref,
					})
				}
				return nil
			})
			if err != nil {
				return nil, err
			}

			childCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			fileUploader := make([]Future, 0, len(files))
			for _, f := range files {
				pth := f.path
				ref := f.ref
				size := f.info.Size()
				fileUploader = append(fileUploader, NewAsyncFuture(childCtx, func(i2 context.Context) (i interface{}, e error) {
					return nil, UploadFile(i2, pth, ref, size, u.store)
				}))
			}

			for _, f := range fileUploader {
				// TODO maybe we should have timeouts, or we can have a global timeout at the top level
				_, err := f.Get(ctx)
				if err != nil {
					return nil, err
				}
			}

			return utils.MakeLiteralForBlob(toPath, false, ""), nil
		} else {
			size := info.Size()
			// Should we make this a go routine as well, so that we can introduce timeouts
			return utils.MakeLiteralForBlob(toPath, false, ""), UploadFile(ctx, fpath, toPath, size, u.store)
		}
	}
}

func (u Uploader) RecursiveUpload(ctx context.Context, vars *core.VariableMap, fromPath string, metadataPath, dataRawPath storage.DataReference) error {
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errFile := path.Join(fromPath, u.errorFileName)
	if info, err := os.Stat(errFile); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else if info.Size() > 1024*1024 {
		return fmt.Errorf("error file too large %d", info.Size())
	} else if info.IsDir() {
		return fmt.Errorf("error file is a directory")
	} else {
		if b, err := ioutil.ReadFile(errFile); err != nil {
			return err
		} else {
			return errors.Errorf("User Error: %s", string(b))
		}
	}

	varFutures := make(map[string]Future, len(vars.Variables))
	for varName, variable := range vars.Variables {
		varPath := path.Join(fromPath, varName)
		varType := variable.GetType()
		switch varType.GetType().(type) {
		case *core.LiteralType_Blob:
			var varOutputPath storage.DataReference
			var err error
			if varName == u.aggregateOutputFileName {
				varOutputPath, err = u.store.ConstructReference(ctx, dataRawPath, "_"+varName)
			} else {
				varOutputPath, err = u.store.ConstructReference(ctx, dataRawPath, varName)
			}
			if err != nil {
				return err
			}
			varFutures[varName] = NewAsyncFuture(childCtx, func(ctx2 context.Context) (interface{}, error) {
				return u.handleBlobType(ctx2, varPath, varOutputPath)
			})
		case *core.LiteralType_Simple:
			varFutures[varName] = NewAsyncFuture(childCtx, func(ctx2 context.Context) (interface{}, error) {
				return u.handleSimpleType(ctx2, varType.GetSimple(), varPath)
			})
		default:
			return fmt.Errorf("currently CoPilot uploader does not support [%s], system error", varType)
		}
	}

	outputs := &core.LiteralMap{
		Literals: make(map[string]*core.Literal, len(varFutures)),
	}
	for k, f := range varFutures {
		logger.Infof(ctx, "Waiting for [%s] to complete (it may have a background upload too)", k)
		v, err := f.Get(ctx)
		if err != nil {
			logger.Errorf(ctx, "Failed to upload [%s], reason [%s]", k, err)
			return err
		}
		l, ok := v.(*core.Literal)
		if !ok {
			return fmt.Errorf("IllegalState, expected core.Literal, received [%s]", reflect.TypeOf(v))
		}
		outputs.Literals[k] = l
		logger.Infof(ctx, "Var [%s] completed", k)
	}

	toOutputPath, err := u.store.ConstructReference(ctx, metadataPath, u.aggregateOutputFileName)
	if err != nil {
		return err
	}
	logger.Infof(ctx, "Uploading final outputs to [%s]", toOutputPath)
	if err := u.store.WriteProtobuf(ctx, toOutputPath, storage.Options{}, outputs); err != nil {
		logger.Errorf(ctx, "Failed to upload final outputs file to [%s], err [%s]", toOutputPath, err)
		return err
	}
	logger.Infof(ctx, "Uploaded final outputs to [%s]", toOutputPath)
	return nil
}

func NewUploader(_ context.Context, store *storage.DataStore, format Format, errorFileName string) Uploader {
	return Uploader{
		format:                  format,
		store:                   store,
		aggregateOutputFileName: "outputs.pb",
		errorFileName:           errorFileName,
	}
}
