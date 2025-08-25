package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"
)

const (
	SNAPSHOT_HEADER_IDENTIFIER = "DDDDSNAP"
	TRAILER_RECORD_IDENTIFIER  = "99999999"
	COMPANY_RECORD_TYPE        = '1'
	PERSON_RECORD_TYPE         = '2'
	BATCH_SIZE                 = 50000
	BUFFER_SIZE                = 16 * 1024 * 1024
)

var (
	companiesCSV    strings.Builder
	personsCSV      strings.Builder
	companiesBatch  = make([]string, 0, BATCH_SIZE)
	personsBatch    = make([]string, 0, BATCH_SIZE)
	companiesHeader = "Company Number,Company Status,Number of Officers,Company Name\n"
	personsHeader   = "Company Number,App Date Origin,Appointment Type,Person number,Corporate indicator,Appointment Date,Resignation Date,Person Postcode,Partial Date of Birth,Full Date of Birth,Title,Forenames,Surname,Honours,Care_of,PO_box,Address line 1,Address line 2,Post_town,County,Country,Occupation,Nationality,Resident Country\n"
)

func fastAtoi(s string) int {
	result := 0
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			result = result*10 + int(s[i]-'0')
		}
	}
	return result
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func fastSubstring(s string, start, end int) string {
	if end > len(s) {
		end = len(s)
	}
	if start >= end {
		return ""
	}
	return s[start:end]
}

func escapeCSVField(s string) string {
	if strings.ContainsAny(s, ",\"\n\r") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

func processHeaderRow(row string) error {
	if !strings.HasPrefix(row, SNAPSHOT_HEADER_IDENTIFIER) {
		return fmt.Errorf("unsupported file type from header: '%s'", row[:8])
	}
	runNumber := row[8:12]
	productionDate := row[12:20]
	fmt.Printf("Processing snapshot file with run number %s from date %s\n", runNumber, productionDate)
	return nil
}

func processCompanyRowUltraFast(row string) string {
	companyNumber := row[:8]
	companyStatus := string(row[9])
	numberOfOfficers := fastAtoi(row[32:36])
	nameLength := fastAtoi(row[36:40])
	companyName := fastSubstring(row, 40, 40+nameLength)
	if len(companyName) > 0 {
		companyName = strings.TrimRight(companyName, " ")
	}
	return fmt.Sprintf("%s,%s,%d,%s\n", companyNumber, companyStatus, numberOfOfficers, escapeCSVField(companyName))
}

func processPersonRowUltraFast(row string) string {
	companyNumber := fastSubstring(row, 0, 8)
	appDateOrigin := fastSubstring(row, 8, 9)
	appointmentType := fastSubstring(row, 10, 12)
	personNumber := fastSubstring(row, 12, 24)
	corporateIndicator := fastSubstring(row, 24, 25)
	appointmentDate := fastSubstring(row, 25, 33)
	resignationDate := fastSubstring(row, 33, 41)
	postcode := fastSubstring(row, 41, 49)
	partialDOB := fastSubstring(row, 49, 57)
	fullDOB := fastSubstring(row, 57, 65)
	variableDataLength := fastAtoi(fastSubstring(row, 65, 69))
	variableData := fastSubstring(row, 69, 69+variableDataLength)
	parts := make([]string, 14)
	if variableData != "" {
		// Trim trailing '<' to avoid empty fields
		variableData = strings.TrimRight(variableData, "<")
		splitParts := strings.SplitN(variableData, "<", 15)
		for i := 0; i < len(splitParts) && i < 14; i++ {
			parts[i] = strings.TrimSpace(splitParts[i])
		}
	}
	var result strings.Builder
	result.Grow(256)
	result.WriteString(escapeCSVField(companyNumber))
	result.WriteByte(',')
	result.WriteString(escapeCSVField(appDateOrigin))
	result.WriteByte(',')
	result.WriteString(escapeCSVField(appointmentType))
	result.WriteByte(',')
	result.WriteString(escapeCSVField(personNumber))
	result.WriteByte(',')
	result.WriteString(escapeCSVField(corporateIndicator))
	result.WriteByte(',')
	result.WriteString(escapeCSVField(appointmentDate))
	result.WriteByte(',')
	result.WriteString(escapeCSVField(resignationDate))
	result.WriteByte(',')
	result.WriteString(escapeCSVField(postcode))
	result.WriteByte(',')
	result.WriteString(escapeCSVField(partialDOB))
	result.WriteByte(',')
	result.WriteString(escapeCSVField(fullDOB))
	for i := 0; i < 14; i++ {
		result.WriteByte(',')
		result.WriteString(escapeCSVField(parts[i]))
	}
	result.WriteByte('\n')
	return result.String()
}

func writeCSVBatch(filename string, header string, batch []string) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriterSize(file, 4*1024*1024)
	if fileInfo, err := file.Stat(); err == nil && fileInfo.Size() == 0 {
		writer.WriteString(header)
	}
	for _, row := range batch {
		writer.WriteString(row)
	}
	return writer.Flush()
}

func processCompanyAppointmentsData(inputFile *os.File, outputFolder, baseInputName string) int {
	companiesProcessed := 0
	personsProcessed := 0
	var wg sync.WaitGroup

	companiesFilename := filepath.Join(outputFolder, fmt.Sprintf("companies_data_%s.csv", baseInputName))
	personsFilename := filepath.Join(outputFolder, fmt.Sprintf("persons_data_%s.csv", baseInputName))
	fmt.Printf("Saving companies data to %s\n", companiesFilename)
	fmt.Printf("Saving persons data to %s\n", personsFilename)

	if err := os.MkdirAll(outputFolder, 0755); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		return 1
	}

	if err := os.WriteFile(companiesFilename, []byte(companiesHeader), 0644); err != nil {
		fmt.Printf("Error initializing companies file: %v\n", err)
		return 1
	}
	if err := os.WriteFile(personsFilename, []byte(personsHeader), 0644); err != nil {
		fmt.Printf("Error initializing persons file: %v\n", err)
		return 1
	}

	scanner := bufio.NewScanner(inputFile)
	scanner.Buffer(make([]byte, BUFFER_SIZE), BUFFER_SIZE*2)

	rowNum := 0
	for scanner.Scan() {
		row := scanner.Text()
		if rowNum == 0 {
			if err := processHeaderRow(row); err != nil {
				fmt.Printf("Error: %v\n", err)
				return 1
			}
			rowNum++
			continue
		}

		if strings.HasPrefix(row, TRAILER_RECORD_IDENTIFIER) {
			wg.Wait()
			if len(companiesBatch) > 0 {
				wg.Add(1)
				batch := companiesBatch
				companiesBatch = companiesBatch[:0]
				go func() {
					defer wg.Done()
					if err := writeCSVBatch(companiesFilename, companiesHeader, batch); err != nil {
						fmt.Printf("Error writing companies batch: %v\n", err)
					}
				}()
			}
			if len(personsBatch) > 0 {
				wg.Add(1)
				batch := personsBatch
				personsBatch = personsBatch[:0]
				go func() {
					defer wg.Done()
					if err := writeCSVBatch(personsFilename, personsHeader, batch); err != nil {
						fmt.Printf("Error writing persons batch: %v\n", err)
					}
				}()
			}
			wg.Wait()
			recordCount := fastAtoi(row[8:16])
			totalProcessed := companiesProcessed + personsProcessed
			if recordCount != totalProcessed {
				fmt.Printf("ERROR: Processed %d records out of %d\n", totalProcessed, recordCount)
				return 1
			}
			fmt.Printf("Processed %d records: %d companies, %d persons.\n", totalProcessed, companiesProcessed, personsProcessed)
			return 0
		}

		if len(row) > 8 {
			recordType := row[8]
			if recordType == COMPANY_RECORD_TYPE {
				companiesBatch = append(companiesBatch, processCompanyRowUltraFast(row))
				companiesProcessed++
				if len(companiesBatch) >= BATCH_SIZE {
					wg.Add(1)
					batch := companiesBatch
					companiesBatch = companiesBatch[:0]
					go func() {
						defer wg.Done()
						if err := writeCSVBatch(companiesFilename, companiesHeader, batch); err != nil {
							fmt.Printf("Error writing companies batch: %v\n", err)
						}
					}()
				}
			} else if recordType == PERSON_RECORD_TYPE {
				personsBatch = append(personsBatch, processPersonRowUltraFast(row))
				personsProcessed++
				if len(personsBatch) >= BATCH_SIZE {
					wg.Add(1)
					batch := personsBatch
					personsBatch = personsBatch[:0]
					go func() {
						defer wg.Done()
						if err := writeCSVBatch(personsFilename, personsHeader, batch); err != nil {
							fmt.Printf("Error writing persons batch: %v\n", err)
						}
					}()
				}
			}
		}
		rowNum++
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return 1
	}

	fmt.Println("ERROR: No trailer record found.")
	return 1
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: ./parser input_file output_folder")
		os.Exit(1)
	}

	inputFilename := os.Args[1]
	outputFolder := os.Args[2]

	inputFile, err := os.Open(inputFilename)
	if err != nil {
		fmt.Printf("Error opening input file: %v\n", err)
		os.Exit(1)
	}
	defer inputFile.Close()

	baseInputName := strings.TrimSuffix(filepath.Base(inputFilename), filepath.Ext(inputFilename))
	os.Exit(processCompanyAppointmentsData(inputFile, outputFolder, baseInputName))
}
