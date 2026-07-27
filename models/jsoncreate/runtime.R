# jsoncreate - final step of the sample processing pipeline.
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

jsoncreate<-function(collection_id, sample_id){
  #Example:
  #sample_id=23598
  #collection_id=2290

  # Load from local storage first; fall back to S3 for legacy samples
  local_componentize_path <- paste0("/data/index/", sample_id, "/", sample_id, "-componentize.RData")
  if (file.exists(local_componentize_path)) {
    cat("Loading componentize.RData from local:", local_componentize_path, "\n")
    load(local_componentize_path)
  } else {
    # Download from S3 to local, then load
    s3_url <- paste0("https://files.dsfp.norman-data.eu/index/", sample_id, "/", sample_id, "-componentize.RData")
    cat("Downloading componentize.RData from S3:", s3_url, "\n")
    dir.create(dirname(local_componentize_path), recursive = TRUE, showWarnings = FALSE)
    download.file(s3_url, local_componentize_path, mode = "wb")
    load(local_componentize_path)
  }
  
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
  
  #save.image("env_for_debugging.RData") #For debugging only
  #load.image("env_for_debugging.RData") #For debugging only

  print("ok")
  w<-which(files$sample_id==sample_id)
  print("ok2")

  #Convert output to JSON
  library("rjson")
  print("ok3")
  
  
  a1<-if(is.na(samples$ID[which(samples$ID==files$sample_id[w])])) "" else samples$ID[which(samples$ID==files$sample_id[w])]
  print("ok_a1")
  
  a2<-if(is.na(samples$"Short name for contribution"[which(samples$ID==files$sample_id[w])])) "" else samples$"Short name for contribution"[which(samples$ID==files$sample_id[w])]
  print("ok_a2")
  
  a3<-if(is.na(instrument_setups$collection_id[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$collection_id[which(instrument_setups$sample_id==files$sample_id[w])]
  print("ok_a3")
  
  a4<-if(is.na(instrument_setups$collection_uuid[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$collection_uuid[which(instrument_setups$sample_id==files$sample_id[w])]
  print("ok_a4")
  
  a5<-if(is.na(samples$Collection[which(samples$ID==files$sample_id[w])])) "" else samples$Collection[which(samples$ID==files$sample_id[w])]
  print("ok_a4")
  
  a6<-if(is.na(samples$"Sample type"[which(samples$ID==files$sample_id[w])])) "" else samples$"Sample type"[which(samples$ID==files$sample_id[w])]
  print("ok_a6")
  
  a7<-if(is.na(samples$"Monitored City"[which(samples$ID==files$sample_id[w])])) "" else samples$"Monitored City"[which(samples$ID==files$sample_id[w])]
  print("ok_a7")
  
  a8<-if(is.na(samples$"Monitored country"[which(samples$ID==files$sample_id[w])])) "" else samples$"Monitored country"[which(samples$ID==files$sample_id[w])]
  print("ok_a8")
  
  a9<-if(length(samples$Latitude[which(samples$ID==files$sample_id[w])])){
      if(is.na(samples$Latitude[which(samples$ID==files$sample_id[w])])){
        "" 
      } else {
        samples$Latitude[which(samples$ID==files$sample_id[w])]
      }
    } else {
      ""
    }
  print("ok_a9")
  
  
  a10<-if(length(samples$Longitude[which(samples$ID==files$sample_id[w])])==1){
    if(is.na(samples$Longitude[which(samples$ID==files$sample_id[w])])){
      "" 
    } else {
      samples$Longitude[which(samples$ID==files$sample_id[w])]
    }
  } else {
    ""
  }
  print("ok_a10")

  a11<-if(is.na(input$"Sampling date")) "" else format(as.Date(trimws(unlist(strsplit(unlist(strsplit(input$"Sampling date",","))[2],"-"))[1], "both"),                                                                                     tryFormats = c("%m/%d/%Y")), format="%Y-%m-%d")
  a12<-if(is.na(input$"Sampling date")) "" else unlist(strsplit(input$"Sampling date","- "))[length(unlist(strsplit(input$"Sampling date","- ")))]
  a13<-if(is.na(input$"Analysis date")) "" else format(as.Date(trimws(unlist(strsplit(unlist(strsplit(input$"Analysis date",","))[2],"-"))[1], "both"),tryFormats = c("%m/%d/%Y")), format="%Y-%m-%d")
  a14<-if(is.na(input$"Analysis date")) "" else unlist(strsplit(input$"Analysis date","- "))[length(unlist(strsplit(input$"Analysis date","- ")))]
  
  tmp_matrix<-files$matrix[which(files$sample_id==sample_id)]
  a14_2<-if(is.na(tmp_matrix)) "" else tmp_matrix
  
  tmp_matrix2<-files$matrix_type[which(files$sample_id==sample_id)]
  a14_3<-if(is.na(tmp_matrix2)) "" else tmp_matrix2
  
  
  
  a15<-if(is.na(instrument_setups$setup_id[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$setup_id[which(instrument_setups$sample_id==files$sample_id[w])]
  a16<-if(is.na(instrument_setups$setup_alias[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$setup_alias[which(instrument_setups$sample_id==files$sample_id[w])]
  a17<-if(is.na(instrument_setups$instrument[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$instrument[which(instrument_setups$sample_id==files$sample_id[w])]
  a18<-if(is.na(instrument_setups$instrument_type[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$instrument_type[which(instrument_setups$sample_id==files$sample_id[w])]
  a19<-if(is.na(instrument_setups$ionization_type[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$ionization_type[which(instrument_setups$sample_id==files$sample_id[w])]
  a20<-if(is.na(instrument_setups$column_model[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$column_model[which(instrument_setups$sample_id==files$sample_id[w])]
  a21<-if(is.na(instrument_setups$column_manufacturer[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$column_manufacturer[which(instrument_setups$sample_id==files$sample_id[w])]
  a22<-if(is.na(instrument_setups$reconsitution_solvent[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$reconsitution_solvent[which(instrument_setups$sample_id==files$sample_id[w])]
  a23<-if(is.na(instrument_setups$ppm_tolerance[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$ppm_tolerance[which(instrument_setups$sample_id==files$sample_id[w])]
  
  print("ok6")
  pointer_instrument<-which(instrument$instrument_id==instrument_setups$instrument[which(instrument_setups$sample_id==files$sample_id[w])])
  a24<-if(is.na(instrument_setups$peakwidth_min[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$peakwidth_min[which(instrument_setups$sample_id==files$sample_id[w])]
  a25<-if(is.na(instrument_setups$peakwidth_max[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$peakwidth_max[which(instrument_setups$sample_id==files$sample_id[w])]
  a26<-if(is.na(instrument_setups$minimum_number_of_scans[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$minimum_number_of_scans[which(instrument_setups$sample_id==files$sample_id[w])]
  a27<-if(is.na(instrument_setups$minimum_intensity[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$minimum_intensity[which(instrument_setups$sample_id==files$sample_id[w])]
  a28<-if(is.na(instrument_setups$expression_of_mass_accuracy[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$expression_of_mass_accuracy[which(instrument_setups$sample_id==files$sample_id[w])]
  a29<-if(is.na(instrument_setups$accuracy_full_scan_ms[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$accuracy_full_scan_ms[which(instrument_setups$sample_id==files$sample_id[w])]
  a30<-if(is.na(instrument_setups$accuracy_ms_ms[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$accuracy_ms_ms[which(instrument_setups$sample_id==files$sample_id[w])]
  a31<-if(is.na(instrument_setups$electronic_noise_cutoff[which(instrument_setups$sample_id==files$sample_id[w])])) "" else instrument_setups$electronic_noise_cutoff[which(instrument_setups$sample_id==files$sample_id[w])]
  a32<-instrument$instrument_id[pointer_instrument]
  a33<-if(is.na(instrument$lab[pointer_instrument])) "" else instrument$lab[pointer_instrument]
  a34<-if(is.na(instrument$organisation[pointer_instrument])) "" else instrument$organisation[pointer_instrument]
  a35<-if(is.na(instrument$"organisation_abbreviation"[pointer_instrument])) "" else instrument$"organisation_abbreviation"[pointer_instrument]
  a36<-if(is.na(instrument$instrument_title[pointer_instrument])) "" else instrument$instrument_title[pointer_instrument]
  a37<-if(is.na(instrument$instrument_manufacturer[pointer_instrument])) "" else instrument$instrument_manufacturer[pointer_instrument]
  a38<-if(is.na(instrument$instrument_model[pointer_instrument])) "" else instrument$instrument_model[pointer_instrument]
  a39<-if(is.na(instrument$short_description[pointer_instrument])) "" else instrument$short_description[pointer_instrument]
  
  print("ok7")
  print("beforepaste0")

  # Helpers for hand-built JSON. paste0() does not escape string values, so
  # any " or \ in a field value produces structurally invalid JSON.
  .normalize_json_value <- function(x) {
    if (is.null(x) || length(x) == 0) return(NA_character_)
    x <- as.character(x[1])
    if (is.na(x)) return(NA_character_)
    x <- trimws(x)
    if (identical(x, "")) return(NA_character_)
    x
  }
  .json_str <- function(x) {
    x <- .normalize_json_value(x)
    if (is.na(x)) return('""')
    x <- gsub("\\\\", "\\\\\\\\", x)  # \ -> \\
    x <- gsub('"',    '\\\\"',    x)  # " -> \"
    x <- gsub("\n",  "\\\\n",    x)  # newline -> \n
    x <- gsub("\r",  "\\\\r",    x)  # CR -> \r
    x <- gsub("\t",  "\\\\t",    x)  # tab -> \t
    paste0('"', x, '"')
  }
  .json_num <- function(x) {
    # Returns "null" (JSON null) when a numeric lookup yields nothing,
    # preventing the bare "field": , construct which is invalid JSON.
    x <- .normalize_json_value(x)
    if (is.na(x)) return("null")
    x <- suppressWarnings(as.numeric(x))
    if (is.na(x)) return("null")
    format(x, scientific = FALSE, trim = TRUE)
  }

  beginning_json<-paste0(
    '{ \n"sample_id": ', .json_num(a1), ',\n',
    '"short_name": ', .json_str(a2), ',\n',
    '"collection_id": ', .json_num(a3), ',\n',
    '"collection_uid": ', .json_str(a4), ',\n',
    '"collection_title": ', .json_str(a5), ',\n',
    '"sample_type": ', .json_str(a6), ',\n',
    '"monitored_city": ', .json_str(a7), ',\n',
    '"monitored_country": ', .json_str(a8), ',\n',
    '"latitude": ', .json_str(a9), ',\n',
    '"longitude": ', .json_str(a10), ',\n',
    '"sampling_date": ', .json_str(a11), ',\n',
    '"sampling_time": ', .json_str(a12), ',\n',
    '"analysis_date": ', .json_str(a13), ',\n',
    '"analysis_time": ', .json_str(a14), ',\n',
    '"matrix_type": ', .json_str(a14_2), ',\n',
    '"matrix_type2": ', .json_str(a14_3), ',\n',
    '"instrument_setup_used": {\n',
    '  "setup_id": ', .json_num(a15), ',\n',
    '  "setup_alias": ', .json_str(a16), ',\n',
    '  "instrument": ', .json_num(a17), ',\n',
    '  "instrument_type": ', .json_str(a18), ',\n',
    '  "ionization_type": ', .json_str(a19), ',\n',
    '  "column_model": ', .json_str(a20), ',\n',
    '  "column_manufacturer": ', .json_str(a21), ',\n',
    '  "reconsitution_solvent": ', .json_str(a22), ',\n',
    '  "ppm_tolerance": ', .json_num(a23), ',\n',
    '  "peakwidth_min": ', .json_num(a24), ',\n',
    '  "peakwidth_max": ', .json_num(a25), ',\n',
    '  "minimum_number_of_scans": ', .json_num(a26), ',\n',
    '  "minimum_intensity": ', .json_num(a27), ',\n',
    '  "expression_of_mass_accuracy": ', .json_str(a28), ',\n',
    '  "accuracy_full_scan_ms": ', .json_num(a29), ',\n',
    '  "accuracy_ms_ms": ', .json_num(a30), ',\n',
    '  "electronic_noise_cutoff": ', .json_num(a31), ',\n',
    '  "instrument_id": ', .json_num(a32), ',\n',
    '  "lab": ', .json_str(a33), ',\n',
    '  "organisation": ', .json_str(a34), ',\n',
    '  "organisation_abbreviation": ', .json_str(a35), ',\n',
    '  "instrument_title": ', .json_str(a36), ',\n',
    '  "instrument_manufacturer": ', .json_str(a37), ',\n',
    '  "instrument_model": ', .json_str(a38), ',\n',
    '  "short_description": ', .json_str(a39), '\n',
    '  },'
  )
  
  print("ok3")
  
  i<-1
  json<-c()
  for(i in 1:nrow(formatedoutput)){
    y<-toJSON(formatedoutput[i,-which(names(formatedoutput)=="Component information")])
    
    
    has_isotopes<-grepl(x=formatedoutput$`Component information`[i], pattern="Isotopes:")
    has_adducts<-grepl(x=formatedoutput$`Component information`[i], pattern="Adducts:")
    has_HRMSMS<-grepl(x=formatedoutput$`Component information`[i], pattern="HRMS/MS:")
    
    temp_isotopes<-c()
    temp_adducts<-c()
    HRMSMS<-c()
    temp<-c()
    if(has_HRMSMS & !has_adducts & !has_isotopes){
      HRMSMS<-unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:"))[length(unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:")))]
      HRMSMS<-do.call(rbind.data.frame, strsplit(unlist(strsplit(HRMSMS,","))," "))
      names(HRMSMS)<-c("mz","int")
      
      temp_isotopes<-data.frame("mz"=0, "int"=0, "rt"=0)
      temp_isotopes<-temp_isotopes[-1,]
      
      temp_adducts<-data.frame("mz"=0, "int"=0, "rt"=0)
      temp_adducts<-temp_adducts[-1,]
    } else if(has_HRMSMS & has_adducts & has_isotopes){
      HRMSMS<-unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:"))[length(unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:")))]
      HRMSMS<-do.call(rbind.data.frame, strsplit(unlist(strsplit(HRMSMS,","))," "))
      names(HRMSMS)<-c("mz","int")
      
      temp<-unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:"))[1]
      
      temp_adducts<-unlist(strsplit(temp,"Adducts:"))[length(unlist(strsplit(temp,"Adducts:")))]
      temp_adducts<-do.call(rbind.data.frame, strsplit(unlist(strsplit(temp_adducts,"\n"))," "))
      names(temp_adducts)<-c("mz","rt","int")
      
      temp_isotopes<-unlist(strsplit(temp,"Adducts:"))[1]
      temp_isotopes<-gsub(x=temp_isotopes, pattern="Isotopes:", replacement = "")
      temp_isotopes<-do.call(rbind.data.frame, strsplit(unlist(strsplit(temp_isotopes,"\n"))," "))
      names(temp_isotopes)<-c("mz","int","rt")
    } else if(has_HRMSMS & has_adducts & !has_isotopes){
      HRMSMS<-unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:"))[length(unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:")))]
      HRMSMS<-do.call(rbind.data.frame, strsplit(unlist(strsplit(HRMSMS,","))," "))
      names(HRMSMS)<-c("mz","int")
      
      temp_adducts<-unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:"))[1]
      temp_adducts<-gsub(x=temp_adducts, pattern="Adducts:", replacement="")
      temp_adducts<-do.call(rbind.data.frame, strsplit(unlist(strsplit(temp_adducts,"\n"))," "))
      names(temp_adducts)<-c("mz","int","rt")
      
      temp_isotopes<-data.frame("mz"=0, "int"=0, "rt"=0)
      temp_isotopes<-temp_isotopes[-1,]
    } else if(has_HRMSMS & !has_adducts & has_isotopes){
      HRMSMS<-unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:"))[length(unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:")))]
      HRMSMS<-do.call(rbind.data.frame, strsplit(unlist(strsplit(HRMSMS,","))," "))
      names(HRMSMS)<-c("mz","int")
      
      temp_isotopes<-unlist(strsplit(formatedoutput$`Component information`[i],"HRMS/MS:"))[1]
      temp_isotopes<-gsub(x=temp_isotopes, pattern="Isotopes:", replacement = "")
      temp_isotopes<-do.call(rbind.data.frame, strsplit(unlist(strsplit(temp_isotopes,"\n"))," "))
      names(temp_isotopes)<-c("mz","int","rt")
      
      temp_adducts<-data.frame("mz"=0, "int"=0, "rt"=0)
      temp_adducts<-temp_adducts[-1,]
    } else if(!has_HRMSMS & has_adducts & !has_isotopes){
      temp_adducts<-gsub(x=formatedoutput$`Component information`[i],pattern="Adducts:",replacement="")
      temp_adducts<-do.call(rbind.data.frame, strsplit(unlist(strsplit(temp_adducts,"\n"))," "))
      names(temp_adducts)<-c("mz","int","rt")
      
      HRMSMS<-data.frame("mz"=0,"int"=0)
      HRMSMS<-HRMSMS[-1,]
      
      temp_isotopes<-data.frame("mz"=0, "int"=0, "rt"=0)
      temp_isotopes<-temp_isotopes[-1,]
    } else if(!has_HRMSMS & !has_adducts & has_isotopes){
      temp_isotopes<-gsub(x=formatedoutput$`Component information`[i],pattern="Isotopes:",replacement="")
      temp_isotopes<-do.call(rbind.data.frame, strsplit(unlist(strsplit(temp_isotopes,"\n"))," "))
      names(temp_isotopes)<-c("mz","int","rt")
      
      HRMSMS<-data.frame("mz"=0,"int"=0)
      HRMSMS<-HRMSMS[-1,]
      
      temp_adducts<-data.frame("mz"=0, "int"=0, "rt"=0)
      temp_adducts<-temp_adducts[-1,]
    } else if(!has_HRMSMS & has_adducts & has_isotopes){
      temp_adducts<-unlist(strsplit(formatedoutput$`Component information`[i],"Adducts:"))[length(unlist(strsplit(formatedoutput$`Component information`[i],"Adducts:")))]
      temp_adducts<-do.call(rbind.data.frame, strsplit(unlist(strsplit(temp_adducts,"\n"))," "))
      names(temp_adducts)<-c("mz","int","rt")
      
      temp_isotopes<-unlist(strsplit(formatedoutput$`Component information`[i],"Adducts:"))[1]
      temp_isotopes<-gsub(x=temp_isotopes,pattern="Isotopes:",replacement="")
      temp_isotopes<-do.call(rbind.data.frame, strsplit(unlist(strsplit(temp_isotopes,"\n"))," "))
      names(temp_isotopes)<-c("mz","int","rt")
      
      HRMSMS<-data.frame("mz"=0,"int"=0)
      HRMSMS<-HRMSMS[-1,]
    } else if(!has_HRMSMS & !has_adducts & !has_isotopes){
      #Has no information
      HRMSMS<-data.frame("mz"=0,"int"=0)
      HRMSMS<-HRMSMS[-1,]
      
      temp_isotopes<-data.frame("mz"=0, "int"=0, "rt"=0)
      temp_isotopes<-temp_isotopes[-1,]
      
      temp_adducts<-data.frame("mz"=0, "int"=0, "rt"=0)
      temp_adducts<-temp_adducts[-1,]
    } else {
      #print(i)
    }
    
    #The output of the above workflow are the following:
    #temp_isotopes, temp_adducts, HRMSMS, finallist
    
    formatedoutput_asjson<-formatedoutput[i,c(4,2,5,31,7,9,11:18,12,12,12,21,22)]
    names(formatedoutput_asjson)<-c("mz",
                                    "rt_minutes","max_intensity",
                                    "peak_area","ion_type","ms_ms_available",
                                    "proposed_identification_name",
                                    "molecular_formula",
                                    "smiles",
                                    "norman_id",
                                    "cas",
                                    "estimated_concentration","concentration_units",
                                    "estimated_concentration_method",
                                    "semiquantification_based_on_compound",
                                    "structural_similarity_for_semiquantification",
                                    "identification_confidence",
                                    "lc_retention_index","gc_retention_time_index")
    
    component_info<-rjson::toJSON(formatedoutput_asjson, indent=4) #-which(names(formatedoutput)=="Component information")
    component_info<-gsub(x=component_info, pattern=":    ", replacement = ": ")
    component_info<-gsub(x=component_info, pattern="\n}", replacement = ",")
    
    temp_isotopes$mz<-as.numeric(temp_isotopes$mz)
    temp_isotopes$int<-as.numeric(temp_isotopes$int)
    temp_isotopes$rt<-as.numeric(temp_isotopes$rt)
    names(temp_isotopes)<-c("isotopes_mz","isotopes_int","isotopes_rt")
    
    index<-1
    temp_isotopes_info<-c()
    if(nrow(temp_isotopes)>0){
      temp_isotopes_info<-rjson::toJSON(temp_isotopes, indent=4)
      temp_isotopes_info <-gsub(gsub(temp_isotopes_info, pattern="\n        ", replacement=""),
                                pattern="\n    ],\n",
                                replacement="],\n")
      temp_isotopes_info<-gsub(temp_isotopes_info, pattern="\n    ]\n}", replacement="],\n")
      temp_isotopes_info<-gsub(temp_isotopes_info, pattern='\\{', replacement="")
      temp_isotopes_info<-gsub(temp_isotopes_info, pattern='\n}', replacement=",")
    } else {
      temp_isotopes_info<-"\n    \"isotopes_mz\":[],\n    \"isotopes_int\":[],\n    \"isotopes_rt\":[],\n"
    }
    
    
    temp_adducts$mz<-as.numeric(temp_adducts$mz)
    temp_adducts$int<-as.numeric(temp_adducts$int)
    temp_adducts$rt<-as.numeric(temp_adducts$rt)
    names(temp_adducts)<-c("adducts_mz","adducts_int","adducts_rt")
    index<-1
    temp_adducts_info<-c()
    if(nrow(temp_adducts)>1){
      temp_adducts_info<-rjson::toJSON(temp_adducts, indent=4)
      temp_adducts_info <-gsub(gsub(temp_adducts_info, pattern="\n        ", replacement=""),
                               pattern="\n    ],\n",
                               replacement="],\n")
      temp_adducts_info<-gsub(temp_adducts_info, pattern="\n    ]\n}", replacement="],\n")
      temp_adducts_info<-gsub(temp_adducts_info, pattern='\\{\n', replacement="")
      temp_adducts_info<-gsub(temp_adducts_info, pattern='\n\\}', replacement=",\n")
    } else  if(nrow(temp_adducts)==1){
      temp_adducts_info<-paste0('    \"adducts_mz\":[',temp_adducts[1],'],\n    \"adducts_int\":[',temp_adducts[2],'],\n    \"adducts_rt\":[',temp_adducts[3],'],\n')
    } else {
    temp_adducts_info<-"    \"adducts_mz\":[],\n    \"adducts_int\":[],\n    \"adducts_rt\":[],\n"
  }
    
    
    HRMSMS$mz<-as.numeric(HRMSMS$mz)
    HRMSMS$int<-as.numeric(HRMSMS$int)
    names(HRMSMS)<-c("hrmsms_mz","hrmsms_int")
    
    HRMSMS_info<-c()
    index<-1
    if(nrow(HRMSMS)>1){
      HRMSMS_info <-rjson::toJSON(HRMSMS, indent=4) 
      HRMSMS_info <-gsub(gsub(HRMSMS_info, pattern="\n        ", replacement=""),
                               pattern="\n    ],\n",
                               replacement="],\n")
      HRMSMS_info<-gsub(HRMSMS_info, pattern="\n    ]\n}", replacement="]\n")
      HRMSMS_info<-gsub(HRMSMS_info, pattern='\\{\n', replacement="")
    } else  if(nrow(HRMSMS)==1){
      HRMSMS_info<-paste0(    '\"hrmsms_mz\":[',HRMSMS[1],'],\n    \"hrmsms_int\":[',HRMSMS[2],']\n')
    } else {
      HRMSMS_info<-"    \"hrmsms_mz\":[],\n    \"hrmsms_int\":[]"
    }
    
    #Add sample id here
    if(i==1){ #i==nrow(formatedoutput)
      json[i] <- paste0(
        beginning_json,'\n',
        '"fullscan"', ': [', '\n',
        component_info,
        temp_isotopes_info,
        temp_adducts_info,
        HRMSMS_info,
        '},'    
      )
    } else if(i==nrow(formatedoutput)){
      
      names(finallist)<-c("mz","rt","int","file")
      finallist$mz<-as.numeric(finallist$mz)
      finallist$int<-as.numeric(finallist$int)
      finallist$rt<-as.numeric(finallist$rt)
      finallist$file<-unlist(lapply(strsplit(finallist$file,'/'),function(x){ x[length(x)]}))
      finallist<-finallist
      finallist_info<-rjson::toJSON(finallist, indent=4)
      
      tmp_last <- paste0(
        component_info,
        temp_isotopes_info,
        temp_adducts_info,
        HRMSMS_info,
        '}'    
      )
      
      json[i] <- paste0(tmp_last,
                        '], \n"data_independent": [',
                        finallist_info,
                        ']\n}
                      ')
    } else {
      json[i] <- paste0(
        component_info,
        temp_isotopes_info,
        temp_adducts_info,
        HRMSMS_info,
        '},'    
      )
    }
    
    
    print(paste0("Creating JSON: ",i," out of ",nrow(formatedoutput)))
  }
  
  
  # Write the JSON to local storage. Files will be uploaded to S3 later
  # during the "Prepare" step.
  # Re-running this step for the same sample must replace any previous
  # output, not append to it. write(..., append=TRUE) alone does NOT
  # truncate an existing file, so an old (possibly stale/malformed)
  # standard.json would otherwise remain on disk with the new JSON tacked
  # onto its end, producing a file with multiple concatenated JSON
  # documents. Removing the file first guarantees a clean, single-document
  # result.
  local_output_dir <- paste0("/data/index/", files$sample_id[w])
  dir.create(local_output_dir, recursive = TRUE, showWarnings = FALSE)
  local_output_path <- paste0(local_output_dir, "/", files$sample_id[w], "-standard.json")
  if (file.exists(local_output_path)) {
    file.remove(local_output_path)
  }
  
  lapply(json, write, local_output_path, append = TRUE)
  cat("Saved JSON output to:", local_output_path, "\n")
  
  
  #Command to delete the files
  #unlink(x=paste0("working directory/",new_folder), 
  #       recursive=TRUE,
  #       force = TRUE)
  
  
  
  #end_time <- Sys.time()
  #print(paste0("It took ",round(c(end_time - start_time),2)," min"))
  
  
  
  #}#1:nrow(samples) Loop for all samples within a collection
}

# Entry point: process the single sample passed on the command line.
jsoncreate(collection_id = .cli_args[[1]], sample_id = .cli_args[[2]])





