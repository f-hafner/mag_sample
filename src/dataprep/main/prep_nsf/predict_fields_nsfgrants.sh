# Defining the dataset path
datapath='/mnt/ssd/extract_nsf/tsv_file' # where you store the tab seperated files with the abstracts of the grants -> put this on /mnt/ssd/
outpath='/mnt/ssd/extract_nsf/output' # where you store the fields, also on /mnt/ssd/
resourcepath='/mnt/ssd/code_backup/LanguageApp/resources' # the language model resources also on /mnt/ssd/
app='/mnt/ssd/code_backup/LanguageApp/LanguageSimilarityExample.exe' # the app itself also on /mnt/ssd/
# Activate the right conda environment 
NECESSARYENV='sample-2022.05.25' # either make your own, or add mono to science-career-tempenv 
conda activate $NECESSARYENV
ENV=$(conda info | grep "active environment" | awk '{ gsub("\t active environment : ", ""); print}')
echo "Working in conda environment: $ENV"
 
# loop over abstract files and process each file
for file in "$datapath"/*
do
    echo "Working on file: $file"
    outfile="${file%.tsv}_fos.txt" 
    outfile="${outfile/${datapath}/${outpath}}"  
    outfile="${outfile/"//"//}"  
    echo "Outfile :$outfile"
    mono $app $resourcepath $file $outfile
done