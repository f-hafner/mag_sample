# Defining the dataset path
datapath='/home/ec2-user/SageMaker/extracts/abstracts_microsoft/' # where you store the tab seperated files with the abstracts of the grants -> put this on /mnt/ssd/
outpath='/home/ec2-user/SageMaker/extracts/magfos/' # where you store the fields, also on /mnt/ssd/
resourcepath='/home/ec2-user/SageMaker/LanguageApp/resources' # the language model resources also on /mnt/ssd/
app='/home/ec2-user/SageMaker/LanguageApp/LanguageSimilarityExample.exe' # the app itself also on /mnt/ssd/
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