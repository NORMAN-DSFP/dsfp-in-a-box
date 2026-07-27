# parse - first step of the sample processing pipeline.
#
# Invoked locally by Drupal's RProcessor service as:
#   Rscript runtime.R <collection_id> <sample_id>
#
# AWS credentials are supplied by the caller through the environment variables
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION (read from
# Drupal's settings.php). They are never hardcoded in this file.
.cli_args <- commandArgs(trailingOnly = TRUE)
if (length(.cli_args) < 2) {
  stop("Usage: Rscript runtime.R <collection_id> <sample_id>")
}

parse<-function(collection_id, sample_id){
  #Step 1. Parse CSV files from paths----
  #Example:
  #rm(list=ls())
  #sample_id=23980
  #collection_id=2451

  # mzML files can be 100+ MB — extend the download timeout well beyond the
  # default 60 s so that large files complete over slow/throttled connections.
  options(timeout = 3600)

  instrumentFilePath=paste0("https://dsfp.norman-data.eu/data/",collection_id,"/instruments.csv")
  setupsFilePath=paste0("https://dsfp.norman-data.eu/data/",collection_id,"/instrument-setups.csv") 
  filesFilePath=paste0("https://dsfp.norman-data.eu/data/",collection_id,"/files.csv")
  
  
  library("readr")
  files<-read_csv(filesFilePath)
  files<-as.data.frame(files)
  
  samplesFilePath <- files$samples_file[which(files$sample_id==sample_id)]
  samples<-read_csv(samplesFilePath)
  samples<-as.data.frame(samples)
  
  instrument<-read_csv(instrumentFilePath)
  instrument<-as.data.frame(instrument)
  
  instrument_setups<-read_csv(setupsFilePath)
  instrument_setups<-as.data.frame(instrument_setups)
  if (nrow(instrument_setups) == 0) {
    stop("No instrument setup found for collection ", collection_id,
         " / sample ", sample_id,
         ". Ensure an instrument setup is linked to this sample in Drupal.")
  }
  

  
  
  #Step 2. ----
  #Stores the useful information from the csv files to input R object
  #Stores the RTI calibrants in rti_calibrants2 R object
  #Creates a folder with unique name and download the mzML files locally
  w<-which(files$sample_id==sample_id)
  input<-c()
  randomStrings2 <- function(n = 5000) {
    a <- do.call(paste0, replicate(5, sample(LETTERS, n, TRUE), FALSE))
    paste0(a, sprintf("%04d", sample(9999, n, TRUE)), sample(LETTERS, n, TRUE))
  }
  #for(w in c(1:nrow(files))){ #Loop for all samples within a collection
    
    if(grepl(x=sessionInfo()$running,pattern="Windows")){
      new_folder<-paste0(gsub(x=(format(Sys.time(), "%Y %m %d %H %M %S")), pattern=" ", replacement=""), randomStrings2(n=1))
      dir.create("C:/tmp/working directory")
      mzML_folder <- paste0("C:/tmp/working directory/",new_folder,"/")
      dir.create(mzML_folder)
    } else {
      new_folder<-paste0(gsub(x=(format(Sys.time(), "%Y %m %d %H %M %S")), pattern=" ", replacement=""), randomStrings2(n=1))
      dir.create("/tmp/working directory")
      mzML_folder <- paste0("/tmp/working directory/",new_folder,"/")
      dir.create(mzML_folder)
    }
    
    input$Institute<-paste0(instrument$organisation_abbreviation,collapse="_")
    mzML_files_for_revision <-files[w,-1]
    
    
    #Data dependent file
    input$datadependentpositive<-mzML_files_for_revision$data_dependent
    if(!is.na(input$datadependentpositive)){
      input$doyouhavedatadependentpositivetosubmit<-"Yes"
      
      input$datadependentpositive_local <- strsplit(unlist(lapply(strsplit(input$datadependentpositive,"/"),function(x){ x[length(x)]})),"\\?VersionId=")[[1]][1]
      download.file(url=input$datadependentpositive, destfile=paste0(mzML_folder, input$datadependentpositive_local))
      
    } else {
      input$doyouhavedatadependentpositivetosubmit<-"No"
    }
    
    #Data independent file(s)
    if(!is.na(files$files_per_collision_channel[w])){
      input$howmanychannelsinpositiveionization<-sum(grepl(x=unlist(strsplit(files$files_per_collision_channel[w],"\n")), pattern="https:"))
      a<-trimws(unlist(strsplit(files$files_per_collision_channel[w],"\n")),"both")
      a<-gsub(x=a,pattern=":https:",replacement="https:")
      k<-1
      p<-1
      r<-1
      for(k in 1:length(a)){
        if(grepl(a[k],pattern="https:")){
          input[[paste0("Paths_DataIndependentFiles_POS",p)]]<-a[k]
          
          input[[paste0("dataindependentpositive_local",p)]]<-strsplit(unlist(lapply(strsplit(a[k],"/"),function(x){ x[length(x)]})),"\\?VersionId=")[[1]][1]
          
          download.file(url=input[[paste0("Paths_DataIndependentFiles_POS",p)]], 
                        destfile=paste0(mzML_folder,  input[[paste0("dataindependentpositive_local",p)]]))
          p<-p+1
        } else {
          input[[paste0("DataIndependent_channel",r)]]<-a[k]
          r<-r+1
        }
      }
    } else if(!is.na(files$data_independent[w])){
      input[[paste0("dataindependentpositive_local",1)]]<-strsplit(unlist(lapply(strsplit(files$data_independent[w],"/"),function(x){ x[length(x)]})),"\\?VersionId=")[[1]][1]
      input[[paste0("DataIndependent_channel",1)]]<-"20"
      
      download.file(url=files$data_independent[w],
                    destfile=paste0(mzML_folder,  input[[paste0("dataindependentpositive_local",1)]]))
    }
    
    #Full scan file
    if(is.na(mzML_files_for_revision$data_fullscan)){
      #If no full-scan file, then  consider the data dependent file
      input$fullscanpositive <- mzML_files_for_revision$data_dependent
      if(is.na(input$fullscanpositive)){
        #If no full-scan file and data dependent file check data independent
        if(!is.na(mzML_files_for_revision$files_per_collision_channel) & 
           is.na(mzML_files_for_revision$data_independent)){
          
          CEofchannelspositive<-unlist(lapply(1:input$howmanychannelsinpositiveionization, function(i){ input[[paste0("DataIndependent_channel",i)]] }))
          CEofchannelspositive<-as.numeric(CEofchannelspositive)
          FilesCEofchannelspositive<-unlist(lapply(1:input$howmanychannelsinpositiveionization, function(i){ input[[paste0("Paths_DataIndependentFiles_POS",i)]][1] }))
          
          input$fullscanpositive <- FilesCEofchannelspositive[which.min(CEofchannelspositive)]
        } else if(is.na(mzML_files_for_revision$files_per_collision_channel) & 
                  !is.na(mzML_files_for_revision$data_independent)){
          input$fullscanpositive <- mzML_files_for_revision$data_independent
        } else {
          CEofchannelspositive<-unlist(lapply(1:input$howmanychannelsinpositiveionization, function(i){ input[[paste0("DataIndependent_channel",i)]] }))
          CEofchannelspositive<-as.numeric(CEofchannelspositive)
          FilesCEofchannelspositive<-unlist(lapply(1:input$howmanychannelsinpositiveionization, function(i){ input[[paste0("Paths_DataIndependentFiles_POS",i)]][1] }))
          
          input$fullscanpositive <- FilesCEofchannelspositive[which.min(CEofchannelspositive)]
        }
      }
    } else {
      input$fullscanpositive <- mzML_files_for_revision$data_fullscan
    }
    
    input$fullscanpositive_local <- strsplit(unlist(lapply(strsplit(input$fullscanpositive,"/"),function(x){ x[length(x)]})),"\\?VersionId=")[[1]][1]
    if(!is.na(input$fullscanpositive)) download.file(url=input$fullscanpositive, destfile=paste0(mzML_folder,input$fullscanpositive_local))
    
    input$mDaORppm_POS<-instrument_setups$expression_of_mass_accuracy[which(instrument_setups$sample_id==files$sample_id[w])]
    input$Accuracy_MS_mDa_POS<-instrument_setups$accuracy_full_scan_ms[which(instrument_setups$sample_id==files$sample_id[w])]
    input$electronic_noise_cutoff_POS<-instrument_setups$electronic_noise_cutoff[which(instrument_setups$sample_id==files$sample_id[w])]
    
    input$Country_wheresamplecomesfrom<-samples$`Monitored country`[which(samples$ID==files$sample_id[w])]
    input$Analysis_date<-samples$`Analysis date`[which(samples$ID==files$sample_id[w])]
    input$City<-samples$`Monitored City`[which(samples$ID==files$sample_id[w])]
    
    input$Sampling_date<-samples$`Sampling date`[which(samples$ID==files$sample_id[w])]
    input$Short_Name<-samples$`Short name for contribution`[which(samples$ID==files$sample_id[w])]
    input$Title_Project<-samples$Collection[which(samples$ID==files$sample_id[w])]
    if(is.na(input$Country_wheresamplecomesfrom) | is.na(samples$`Sampling date`[which(samples$ID==files$sample_id[w])])){
      input$unique_name<-substr(new_folder,start=0,stop=13)
    } else {
      input$unique_name<-paste0(substr(input$Country_wheresamplecomesfrom,start=0,stop=1),gsub(as.character(format(as.Date(trimws(unlist(strsplit(unlist(strsplit(samples$`Sampling date`[which(samples$ID==files$sample_id[w])],","))[2],"-"))[1], "both"), tryFormats = c("%m/%d/%Y")), format="%d.%m.%Y")),pattern="-",replacement=""),collapse="")
    }
    
    rti_calibrants<-trimws(unlist(strsplit(instrument_setups$rti_calibrants[which(instrument_setups$sample_id==files$sample_id[w])],"\n ")),"both")
    if(any(rti_calibrants=="No data available.")){
      rti_calibrants<-trimws(unlist(strsplit(instrument_setups$rti_calibrants_negative[which(instrument_setups$sample_id==files$sample_id[w])],"\n ")),"both")
    }
    rti_calibrants_name<-c(rti_calibrants[1],rti_calibrants[11],rti_calibrants[18],
                           rti_calibrants[25],rti_calibrants[32],rti_calibrants[39],rti_calibrants[46],
                           rti_calibrants[53],rti_calibrants[60],rti_calibrants[67],rti_calibrants[74],
                           rti_calibrants[81],rti_calibrants[88],rti_calibrants[95],rti_calibrants[102],
                           rti_calibrants[109],rti_calibrants[116],rti_calibrants[123])
    rti_calibrants_rt<-c(rti_calibrants[2],rti_calibrants[12],rti_calibrants[19],
                         rti_calibrants[26],rti_calibrants[33],rti_calibrants[40],rti_calibrants[47],
                         rti_calibrants[54],rti_calibrants[61],rti_calibrants[68],rti_calibrants[75],
                         rti_calibrants[82],rti_calibrants[89],rti_calibrants[96],rti_calibrants[103],
                         rti_calibrants[110],rti_calibrants[117],rti_calibrants[124])
    rti_calibrants_cas<-c(rti_calibrants[3],rti_calibrants[13],rti_calibrants[20],
                          rti_calibrants[27],rti_calibrants[34],rti_calibrants[41],rti_calibrants[48],
                          rti_calibrants[55],rti_calibrants[62],rti_calibrants[69],rti_calibrants[76],
                          rti_calibrants[83],rti_calibrants[90],rti_calibrants[97],rti_calibrants[104],
                          rti_calibrants[111],rti_calibrants[118],rti_calibrants[125])        
    rti_calibrants_formula<-c(rti_calibrants[4],rti_calibrants[14],rti_calibrants[21],
                              rti_calibrants[28],rti_calibrants[35],rti_calibrants[42],rti_calibrants[49],
                              rti_calibrants[56],rti_calibrants[63],rti_calibrants[70],rti_calibrants[77],
                              rti_calibrants[84],rti_calibrants[91],rti_calibrants[98],rti_calibrants[105],
                              rti_calibrants[112],rti_calibrants[119],rti_calibrants[126])   
    
    
    rti_calibrants2<-data.frame("Name"=rti_calibrants_name,
                                "RT"=rti_calibrants_rt,
                                "CAS"=rti_calibrants_cas,
                                "Formula"=rti_calibrants_formula)
    
    if(all(is.na(rti_calibrants2$RT==""))){
      input$ihavehistoricaldata<-TRUE
    } else {
      input$ihavehistoricaldata<-FALSE
    }
    #IMPROTANT! RI calibrants?
    
    input$ppm_positive<-instrument_setups$ppm_tolerance[which(instrument_setups$sample_id==files$sample_id[w])]
    input$peakwidth_positive<-c(instrument_setups$peakwidth_min[which(instrument_setups$sample_id==files$sample_id[w])],instrument_setups$peakwidth_max[which(instrument_setups$sample_id==files$sample_id[w])])
    input$prefilter<-c(instrument_setups$minimum_number_of_scans[which(instrument_setups$sample_id==files$sample_id[w])],instrument_setups$minimum_intensity[which(instrument_setups$sample_id==files$sample_id[w])])
    
    if(!is.numeric(input$ppm_positive) || is.na(input$ppm_positive)) input$ppm_positive<-22.8
    if(!all(is.numeric(input$peakwidth_positive)) || any(is.na(input$peakwidth_positive)) || length(input$peakwidth_positive) < 2) input$peakwidth_positive<-c(20,60)
    if(!all(is.numeric(input$prefilter)) || any(is.na(input$prefilter)) || length(input$prefilter) < 2) input$prefilter<-c(3, 100)
    
    
    
    input$mztol_componentization<-c(); input$ppm_componentization<-c()
    if(instrument_setups$expression_of_mass_accuracy[which(instrument_setups$sample_id==files$sample_id[w])]=="ppm"){
      input$mztol_componentization<-instrument_setups$accuracy_full_scan_ms[which(instrument_setups$sample_id==files$sample_id[w])]
      input$ppm_componentization<-TRUE
    } else if(instrument_setups$expression_of_mass_accuracy[which(instrument_setups$sample_id==files$sample_id[w])]=="mDa"){
      input$mztol_componentization<-instrument_setups$accuracy_full_scan_ms[which(instrument_setups$sample_id==files$sample_id[w])]/1000
      input$ppm_componentization<-FALSE
    } else {
      input$mztol_componentization<-0.003
      input$ppm_componentization<-FALSE
      #print('Very strange case. Normally should not be here')
    }
    
    input$ionization_type <- instrument_setups$ionization_type[which(instrument_setups$sample_id==files$sample_id[w])]
    input$instrument_type <- instrument_setups$instrument_type[which(instrument_setups$sample_id==files$sample_id[w])]
    input$institutename <- instrument$`Organisation Abbreviation`[which(instrument_setups$sample_id==files$sample_id[w])]
    input$"Sampling date" <- samples$"Sampling date"[which(instrument_setups$sample_id==files$sample_id[w])]
    input$"Analysis date" <- samples$"Analysis date"[which(instrument_setups$sample_id==files$sample_id[w])]
    
    
    #Step 3. Main mechanism for creating component list------
    # library("mzR")
    # library("xcms")
    #library("ChemmineOB")
    #library("ChemmineR")
    library("peakTrAMS")
    library("nontarget")
    #library("shiny")
    #withProgress(message = 'Creating DCT', value = 0, {
    #incProgress(0.01/2, detail = paste("Loading parameters. Please wait..."))
    
    #incProgress(0.25/2, detail = paste("Finding peaks in full-scan data. Please wait..."))
    fullscanpositive<-paste0(mzML_folder,input$fullscanpositive_local)
    
    
    xset <-xcms::xcmsSet(files = fullscanpositive,
                   method = 'centWave', 
                   ppm = input$ppm_positive,
                   prefilter = input$prefilter,
                   peakwidth = input$peakwidth_positive
                   #,mslevel=2
    )
    assign("xset",xset,.GlobalEnv)


    # Write the output to local storage. Files will be uploaded to S3 later
    # during the "Prepare" step.
    local_output_dir <- paste0("/data/index/", files$sample_id[w])
    dir.create(local_output_dir, recursive = TRUE, showWarnings = FALSE)
    local_output_path <- paste0(local_output_dir, "/", files$sample_id[w], "-parse.RData")
    
    save(files, filesFilePath, fullscanpositive, input, instrument, instrument_setups,
         instrumentFilePath, mzML_files_for_revision, mzML_folder, new_folder,
         randomStrings2, rti_calibrants, rti_calibrants_cas, rti_calibrants_formula,
         rti_calibrants_name, rti_calibrants_rt, rti_calibrants2, samples,
         samplesFilePath, setupsFilePath, w, xset,
         file = local_output_path)
    cat("Saved parse output to:", local_output_path, "\n")
    
}


# Entry point: process the single sample passed on the command line.
parse(collection_id = .cli_args[[1]], sample_id = .cli_args[[2]])




