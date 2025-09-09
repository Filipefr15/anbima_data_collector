package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/carlmjohnson/requests"
)

func main() {
	url := "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202508.zip"
	output := "inf_diario_fi_202508.zip"
	dest := "unzipped_files"

	err := downloadFile(url, output)
	if err != nil {
		panic(err)
	}

	err = Unzip(output, dest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Arquivo descompactado em:", dest)

	if err := os.Remove(output); err != nil {
		fmt.Printf("Erro ao excluir o arquivo %s: %v\n", output, err)
	} else {
		fmt.Printf("Arquivo %s excluído com sucesso.\n", output)
	}
}

func downloadFile(url, output string) error {
	f, err := os.Create(output)
	if err != nil {
		return err
	}
	defer f.Close()

	err = requests.
		URL(url).
		ToWriter(f).
		Fetch(context.Background())
	if err != nil {
		return err
	}

	fmt.Println("Download concluído:", output)

	return nil
}

func Unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	os.MkdirAll(dest, 0755)

	// Closure to address file descriptors issue with all the deferred .Close() methods
	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, f.Name)

		// Check for ZipSlip (Directory traversal)
		if !strings.HasPrefix(path, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path: %s", path)
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			os.MkdirAll(filepath.Dir(path), f.Mode())
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}
