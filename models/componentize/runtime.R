# componentize - second step of the sample processing pipeline.
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

componentize<-function(collection_id, sample_id){
  #Example:
  #rm(list=ls())
  #sample_id=23882
  #collection_id=2451
  options(timeout = 300)
  
  # Load from local storage first; fall back to S3 for legacy samples
  local_parse_path <- paste0("/data/index/", sample_id, "/", sample_id, "-parse.RData")
  if (file.exists(local_parse_path)) {
    cat("Loading parse.RData from local:", local_parse_path, "\n")
    load(local_parse_path)
  } else {
    # Download from S3 to local, then load
    s3_url <- paste0("https://files.dsfp.norman-data.eu/index/", sample_id, "/", sample_id, "-parse.RData")
    cat("Downloading parse.RData from S3:", s3_url, "\n")
    dir.create(dirname(local_parse_path), recursive = TRUE, showWarnings = FALSE)
    download.file(s3_url, local_parse_path, mode = "wb")
    load(local_parse_path)
  }

  
  
  w<-which(files$sample_id==sample_id)

  # Extract peaklist from the xcmsSet loaded via parse.RData.
  # The mzML files are NOT re-downloaded here — xset already contains all peak
  # picks; downloading the raw files again would waste ~100 MB of memory per
  # file and contribute nothing to this step.
  # Read xset@peaks exactly ONCE — materialising the full peaks matrix is
  # expensive; previous code did it three times and caused OOM on large samples.
  .all_peaks <- as.data.frame(xset@peaks)
  temp_peaklist <- .all_peaks[, c("mz", "rt", "maxo")]
  # Pre-extract the slices needed after xset is freed, using named columns so
  # the code is robust to varying numbers of extra xcmsSet columns.
  .xset_peaks_area_df     <- .all_peaks[, c("mz", "rt", "maxo", "into")]
  .xset_peaks_insource_df <- .all_peaks[, c("mz", "rt", "maxo")]
  rm(.all_peaks)
  peaklist<-as.data.frame(cbind(mass=temp_peaklist$mz,intensity=temp_peaklist$maxo,rt=temp_peaklist$rt/60))
  peaklist$rt<-round(peaklist$rt,3)

  # Apply the noise cutoff before pattern.search.  nontarget::pattern.search is
  # O(n^2) — filtering low-intensity peaks here can reduce n by >90 % and cuts
  # both runtime and peak RAM substantially.  The cutoff values come from the
  # instrument setup; fall back to 1 (keep everything) when absent.
  noise_cutoff <- if (!is.null(input$electronic_noise_cutoff_POS) &&
                      !is.na(input$electronic_noise_cutoff_POS) &&
                      input$ionization_type == "positive") {
    as.numeric(input$electronic_noise_cutoff_POS)
  } else if (!is.null(input$electronic_noise_cutoff_NEG) &&
             !is.na(input$electronic_noise_cutoff_NEG) &&
             input$ionization_type != "positive") {
    as.numeric(input$electronic_noise_cutoff_NEG)
  } else {
    1
  }
  if (is.finite(noise_cutoff) && noise_cutoff > 1) {
    peaklist <- peaklist[peaklist$intensity >= noise_cutoff, ]
    row.names(peaklist) <- NULL
    cat("Peaklist after noise filter:", nrow(peaklist), "peaks (cutoff =", noise_cutoff, ")\n")
  } else {
    cat("Peaklist (no noise filter applied):", nrow(peaklist), "peaks\n")
  }

  # Hard cap: pattern.search is O(n^2) in RAM; even after the noise cutoff some
  # samples have tens of thousands of peaks and will OOM.  Keep only the top N
  # by intensity — the strongest signals are the ones that matter for isotopic
  # pattern matching and adduct detection.
  .peak_cap <- 5000L
  if (nrow(peaklist) > .peak_cap) {
    peaklist <- peaklist[order(peaklist$intensity, decreasing = TRUE)[seq_len(.peak_cap)], ]
    row.names(peaklist) <- NULL
    cat("Peaklist capped to top", .peak_cap, "peaks by intensity\n")
  }

  assign("peaklist", peaklist, .GlobalEnv)

  # Free xset and temp_peaklist from the LOCAL function environment.
  # IMPORTANT: rm() without envir= removes from the calling (local) scope;
  # using envir=.GlobalEnv (as previous code did) is wrong because load()
  # inside a function puts variables into the local env, not .GlobalEnv.
  # files/samples/instrument/instrument_setups must stay alive for s3save().
  rm(xset, temp_peaklist)
  gc()
  
  #incProgress(0.1/2, detail = paste("Isotope elimination"))
  
  library("enviPat")
  data(isotopes)
  
  if(input$ionization_type=="positive"){
    library("nontarget")
    iso<-nontarget:::make.isos(isotopes,use_isotopes=c("13C","15N","34S","37Cl","81Br"),
                   use_charges=c(           1,    1,    1,    1,     1))
    pattern<-"Error"
    if(is.na(input$electronic_noise_cutoff_POS) || is.null(input$electronic_noise_cutoff_POS)) input$electronic_noise_cutoff_POS<-1
    .ps_retries <- 0L
    while(grepl(x=pattern[1],pattern="Error") && .ps_retries < 20L){
      .ps_retries <- .ps_retries + 1L
      entry<-round(runif(n=1, min=10, max=1000))
      pattern<-try(nontarget:::pattern.search(
        peaklist,
        iso,
        cutint=input$electronic_noise_cutoff_POS, #input$electronic_noise_cutoff_POS
        rttol=c(-0.02,0.02),
        mztol=input$mztol_componentization, #0.002
        ppm=input$ppm_componentization, #FALSE
        mzfrac=0.8,
        inttol=0.2,
        rules=c(TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE),
        deter=FALSE,
        entry=entry)) #round(rnorm(n=1,mean=1000,sd=100)))
      #print(entry)
    }
    if (grepl(x = pattern[1], pattern = "Error")) {
      stop("pattern.search (POS) failed after 20 attempts — peaklist may still be too large for available memory.")
    }
    #print('Isotopic grouping is done')
    
    #incProgress(0.1/2, detail = paste("Adduct search"))   
    
    data(adducts)
    adducts2<-adducts
    adducts2$Name<-gsub(gsub(gsub(gsub(gsub(gsub(gsub(gsub(gsub(adducts$Name,
                                                                pattern="]-2-",replacement=""), #special case [something]-2-
                                                           pattern="]-3-",replacement=""), #special case  [something]-3-
                                                      pattern="]\\+",replacement=""), #[something]+
                                                 pattern="]2\\+",replacement=""), #[something]2+ 
                                            pattern="]3\\+",replacement=""), #[something]3+ 
                                       pattern="]\\-",replacement=""), #[something]-
                                  pattern="]2\\-",replacement=""), #[something]2-
                             pattern="]3\\-",replacement=""), #[something]3-
                        pattern="\\[",replacement="") #Remove the [
    
    
    if(grepl(x=input$instrument_type,pattern="LC")){
      useadducts<-c("M+H","M+NH4","M+Na","M+K")
    } else {
      useadducts<-c("M+","M+H")
    }
    
    
    
    adduct_POS<-nontarget:::adduct.search(
      peaklist,
      adducts=adducts2,
      rttol=0.04,
      mztol=input$mztol_componentization, #0.002
      ppm=input$ppm_componentization, #FALSE
      use_adducts=useadducts,
      ion_mode="positive")
    
    assign("adduct_POS",adduct_POS,.GlobalEnv)
    assign("pattern",pattern,.GlobalEnv) 
    
    #incProgress(0.1/2, detail = paste("Creating component list"))     
    comp<-nontarget:::combine(
      pattern,
      adduct_POS,
      homol = FALSE,
      dont=FALSE,
      rules=c(TRUE,FALSE,FALSE,FALSE))
    assign("comp",comp,.GlobalEnv)
    
  } else {
    library("nontarget")
    iso<-nontarget:::make.isos(isotopes,use_isotopes=c("13C","15N","34S","37Cl","81Br"),use_charges=c(-1,-1,-1,-1,-1))
    pattern_NEG<-"Error"
    if(is.na(input$electronic_noise_cutoff_NEG) || is.null(input$electronic_noise_cutoff_NEG)) input$electronic_noise_cutoff_NEG<-1
    .ps_retries_neg <- 0L
    while(grepl(x=pattern_NEG[1],pattern="Error") && .ps_retries_neg < 20L){
      .ps_retries_neg <- .ps_retries_neg + 1L
      entry<-round(runif(n=1, min=10, max=1000))
      pattern_NEG<-try(nontarget:::pattern.search(
        peaklist,
        iso,
        cutint=input$electronic_noise_cutoff_NEG,#150
        rttol=c(-0.02,0.02),
        mztol=input$mztol_componentization, #0.002
        ppm=input$ppm_componentization, #FALSE
        mzfrac=0.8,
        inttol=0.2,
        rules=c(TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE),
        deter=FALSE,
        entry=entry)) #round(rnorm(n=1,mean=1000,sd=100)))
      #print(entry)
    }
    if (grepl(x = pattern_NEG[1], pattern = "Error")) {
      stop("pattern.search (NEG) failed after 20 attempts — peaklist may still be too large for available memory.")
    }
    print('Isotopic grouping is done')
    pattern<-pattern_NEG
    #incProgress(0.1/2, detail = paste("Adduct search"))   
    
    data(adducts)
    adducts2<-adducts
    adducts2$Name<-gsub(gsub(gsub(gsub(gsub(gsub(gsub(gsub(gsub(adducts$Name,
                                                                pattern="]-2-",replacement=""), #special case [something]-2-
                                                           pattern="]-3-",replacement=""), #special case  [something]-3-
                                                      pattern="]\\+",replacement=""), #[something]+
                                                 pattern="]2\\+",replacement=""), #[something]2+ 
                                            pattern="]3\\+",replacement=""), #[something]3+ 
                                       pattern="]\\-",replacement=""), #[something]-
                                  pattern="]2\\-",replacement=""), #[something]2-
                             pattern="]3\\-",replacement=""), #[something]3-
                        pattern="\\[",replacement="") #Remove the [
    
    if(grepl(x=input$instrument_type,pattern="LC")){
      useadducts<-useadducts<-c("M-H","M+Na-2H","M+K-2H","M+Cl","M+Br")
    } else {
      useadducts<-c("M-","M-H")
    }
    
    
    
    adduct_NEG<-nontarget:::adduct.search(
      peaklist,
      adducts=adducts2,
      rttol=0.04,
      mztol=input$mztol_componentization, #0.002
      ppm=input$ppm_componentization, #FALSE
      use_adducts=useadducts,
      ion_mode="negative")
    adduct<-adduct_NEG
    # assign("adduct_NEG",adduct_NEG,.GlobalEnv)
    # assign("pattern_NEG",pattern_NEG,.GlobalEnv) 
    
    #incProgress(0.1/2, detail = paste("Creating component list"))     
    comp_NEG<-nontarget:::combine(
      pattern,
      adduct,
      homol = FALSE,
      dont=FALSE,
      rules=c(TRUE,FALSE,FALSE,FALSE))
    comp<-comp_NEG
    assign("comp_NEG",comp_NEG,.GlobalEnv)
  }
  
  
  #incProgress(0.1/2, detail = paste("Transforming component list to DCT database"))     
  comp[[1]]->output
  
  
  #New code for improving component information column
  output0<-data.frame(mz=rep(0,length(output$`Component ID |`)),rt=rep(0,length(output$`Component ID |`)),int=rep(0,length(output$`Component ID |`)),
                      adduct=rep(0,length(output$`Component ID |`)),msms2=rep(0,length(output$`Component ID |`)),
                      isotopicpeaks=rep(0,length(output$`Component ID |`)),
                      adductpeaks=rep(0,length(output$`Component ID |`)))
  
  output0[,1]<-output$`HI m/z |`
  output0[,2]<-output$`HI RT |`
  output0[,3]<-output$`Highest intensity (HI) |`
  output0[,4]<-output$`pattern group adduct|`
  # output0[,5]<-output$msms2
  output0[,6]<-output$`ID pattern peaks |`
  output0[,7]<-output$`ID adduct peaks |`
  
  i<-1
  output2<-data.frame(mz=0,rt=0,int=0, adduct=0, msms2=0, isotopicpeaks=0,adductpeaks=0, componentinformation=0)
  while(sum(output0$mz)!=0){
    if(sum(output0[output0$mz[i]==output0$mz & output0$rt[i]==output0$rt & output0$int[i]==output0$int,1])>0){ #Is it zero (this means removed previously)? This is to catch cases of previous zero replacements
      if(length(output0[output0$mz[i]==output0$mz & output0$rt[i]==output0$rt & output0$int[i]==output0$int,1])!=1){ #Is it double registration?
        #############Case of a duplicate registration------
        temp<-output0[output0$mz[i]==output0$mz & output0$rt[i]==output0$rt & output0$int[i]==output0$int,]
        
        #Gather all adduct and isotopes in temp variable
        if(any(temp$adduct!="-")) temp$adduct[1]<-paste(output0$adduct[output0$int[i]==output0$int],collapse=",",sep=",")
        if(any(temp$isotopicpeaks!="-")) temp$isotopicpeaks[1]<-paste(output0$isotopicpeaks[output0$int[i]==output0$int],collapse=",",sep=",")
        if(any(temp$adductpeaks!="-")) temp$adductpeaks[1]<-paste(output0$adductpeaks[output0$int[i]==output0$int],collapse=",",sep=",")
        temp<-temp[1,]
        
        if(any(temp$adductpeaks!="-")){
          z<-unlist(strsplit(temp$adductpeaks[1],","))
          z<-z[z!="-"]
          adducts_temp<-as.numeric(unique(z))
        } else {
          adducts_temp<-c()
        }
        #adducts_temp
        
        if(any(temp$isotopicpeaks!="-")){
          zz<-unlist(strsplit(temp$isotopicpeaks[1],","))
          zz<-zz[zz!="-"]
          isotopes_temp<-as.numeric(unique(zz))
        } else {
          isotopes_temp<-c()
        }
        #isotopes_temp
        
        #Until here, we have isotopes_temp and adducts_temp.
        #Sometimes nontarget R-package mixes adducts and isotopes. Let's separate them.
        if(!is.null(isotopes_temp) | !is.null(adducts_temp)){
          relatedpeaks<-unique(c(adducts_temp,isotopes_temp))
          final_isotopes<-relatedpeaks[abs(peaklist$mass[relatedpeaks]-temp$mz)<7.3] #until 7 isotopic peaks
          
          TestIfMolecularIonisIncluded<-peaklist[final_isotopes,]
          TestIfMolecularIonisIncluded<-TestIfMolecularIonisIncluded[order(TestIfMolecularIonisIncluded$mass),]
          IsMolecularIonIncluded<-any(temp$mz==TestIfMolecularIonisIncluded$mass & temp$rt==TestIfMolecularIonisIncluded$rt & temp$int==TestIfMolecularIonisIncluded$intensity)
          HowManyIsotopesIncluded<-length(final_isotopes)
          if(IsMolecularIonIncluded & HowManyIsotopesIncluded>1){
            #Do nothing, everything is fine
          } else if(IsMolecularIonIncluded & HowManyIsotopesIncluded==1){
            #The molecular ion is the only detected isotope. In this case remove it from isotope list
            final_isotopes<-c()
          } else if(!IsMolecularIonIncluded & HowManyIsotopesIncluded>=1){
            #Molecular ion not included and at least an isotopic peak has been detected, then add it to the isotopic peak list to have it complete
          } else {
            #We see what it will do...
          }
          
          final_adducts<-relatedpeaks[!c(abs(peaklist$mass[relatedpeaks]-temp$mz)<7.3)]
        }
        #final_adducts
        #final_isotopes
        
        
        if(length(final_isotopes)>0){
          final_isotopes_peaklist<-peaklist[final_isotopes,]
          final_isotopes_peaklist<-final_isotopes_peaklist[!duplicated(final_isotopes_peaklist),]
          final_isotopes_peaklist<-final_isotopes_peaklist[order(final_isotopes_peaklist$mass),]
          row.names(final_isotopes_peaklist)<-NULL
          final_isotopes_peaklist$intensity<-round(final_isotopes_peaklist$intensity,0)
          final_isotopes_peaklist$rt<-round(final_isotopes_peaklist$rt,3)
          
          index_vectorize<-1; final_isotopes_peaklist_AsVector<-c()
          for(index_vectorize in 1:nrow(final_isotopes_peaklist)) final_isotopes_peaklist_AsVector[index_vectorize]<-paste(final_isotopes_peaklist$mass[index_vectorize],final_isotopes_peaklist$intensity[index_vectorize],final_isotopes_peaklist$rt[index_vectorize])
          final_isotopes_peaklist_AsVector<-paste0(final_isotopes_peaklist_AsVector, collapse="\n")
          final_isotopes_peaklist_AsVector<-paste0("Isotopes:",final_isotopes_peaklist_AsVector,collapse="")
        }
        #final_isotopes_peaklist_AsVector
        
        
        if(length(final_adducts)>0){
          final_adducts_peaklist<-peaklist[final_adducts,]
          final_adducts_peaklist<-final_adducts_peaklist[!duplicated(final_adducts_peaklist),]
          final_adducts_peaklist<-final_adducts_peaklist[order(final_adducts_peaklist$mass),]
          row.names(final_adducts_peaklist)<-NULL
          final_adducts_peaklist$intensity<-round(final_adducts_peaklist$intensity,0)
          final_adducts_peaklist$rt<-round(final_adducts_peaklist$rt,3)
          
          index_vectorize<-1; final_adducts_peaklist_AsVector<-c()
          for(index_vectorize in 1:nrow(final_adducts_peaklist)) final_adducts_peaklist_AsVector[index_vectorize]<-paste(final_adducts_peaklist$mass[index_vectorize],final_adducts_peaklist$intensity[index_vectorize],final_adducts_peaklist$rt[index_vectorize])
          final_adducts_peaklist_AsVector<-paste0(final_adducts_peaklist_AsVector, collapse="\n")
          final_adducts_peaklist_AsVector<-paste0("Adducts:",final_adducts_peaklist_AsVector,collapse="")
        }
        #final_adducts_peaklist_AsVector
        
        
        if(length(final_isotopes)>0 & length(final_adducts)>0) componentinformation<-paste0(final_isotopes_peaklist_AsVector,"\n",final_adducts_peaklist_AsVector)
        else if(length(final_isotopes)==0 & length(final_adducts)>0) componentinformation<-paste0(final_adducts_peaklist_AsVector)
        else if(length(final_isotopes)>0 & length(final_adducts)==0) componentinformation<-paste0(final_isotopes_peaklist_AsVector)
        else if(length(final_isotopes)==0 & length(final_adducts)==0) componentinformation<-"-"
        
        temp$componentinformation<-componentinformation
        
        if(length(final_isotopes)>0){
          temp$isotopicpeaks[1]<-paste0(isotopes_temp,collapse=",")
        } else {
          temp$isotopicpeaks[1]<-"-"
        }
        
        if(length(final_adducts)>0){
          temp$adductpeaks[1]<-paste0(adducts_temp,collapse=",")
        } else {
          temp$adductpeaks[1]<-"-"
        }
        
        output2[i,]<-temp
        output0[output0$mz[i]==output0$mz & output0$rt[i]==output0$rt & output0$int[i]==output0$int,]<-0
        
      } else {
        #############Case of a non-duplicate registration------
        output0[i,]
        
        temp<-output0[output0$mz[i]==output0$mz & output0$rt[i]==output0$rt & output0$int[i]==output0$int,]
        
        
        if(any(temp$adductpeaks!="-")){
          z<-unlist(strsplit(temp$adductpeaks[1],","))
          z<-z[z!="-"]
          adducts_temp<-as.numeric(unique(z))
        } else {
          adducts_temp<-c()
        }
        #adducts_temp
        
        if(any(temp$isotopicpeaks!="-")){
          zz<-unlist(strsplit(temp$isotopicpeaks[1],","))
          zz<-zz[zz!="-"]
          isotopes_temp<-as.numeric(unique(zz))
        } else {
          isotopes_temp<-c()
        }
        #isotopes_temp
        
        #Until here, we have isotopes_temp and adducts_temp. Sometimes nontarget package mixes adducts and isotopes. Let's separate them
        if(!is.null(isotopes_temp) | !is.null(adducts_temp)){
          relatedpeaks<-unique(c(adducts_temp,isotopes_temp))
          final_isotopes<-relatedpeaks[abs(peaklist$mass[relatedpeaks]-temp$mz)<7.3] #until 7 isotopic peaks
          final_adducts<-relatedpeaks[!c(abs(peaklist$mass[relatedpeaks]-temp$mz)<7.3)]
          
          TestIfMolecularIonisIncluded<-peaklist[final_isotopes,]
          TestIfMolecularIonisIncluded<-TestIfMolecularIonisIncluded[order(TestIfMolecularIonisIncluded$mass),]
          IsMolecularIonIncluded<-any(temp$mz==TestIfMolecularIonisIncluded$mass & temp$rt==TestIfMolecularIonisIncluded$rt & temp$int==TestIfMolecularIonisIncluded$intensity)
          HowManyIsotopesIncluded<-length(final_isotopes)
          if(IsMolecularIonIncluded & HowManyIsotopesIncluded>1){
            #Do nothing, everything is fine
          } else if(IsMolecularIonIncluded & HowManyIsotopesIncluded==1){
            #The molecular ion is the only detected isotope. In this case remove it from isotope list
            final_isotopes<-c()
          } else {
            #We see what it will do...
          }
        }
        #final_isotopes
        #final_adducts
        
        if(length(final_isotopes)>0){
          final_isotopes_peaklist<-peaklist[final_isotopes,]
          final_isotopes_peaklist<-final_isotopes_peaklist[!duplicated(final_isotopes_peaklist),]
          final_isotopes_peaklist<-final_isotopes_peaklist[order(final_isotopes_peaklist$mass),]
          row.names(final_isotopes_peaklist)<-NULL
          final_isotopes_peaklist$intensity<-round(final_isotopes_peaklist$intensity,0)
          final_isotopes_peaklist$rt<-round(final_isotopes_peaklist$rt,3)
          
          index_vectorize<-1; final_isotopes_peaklist_AsVector<-c()
          for(index_vectorize in 1:nrow(final_isotopes_peaklist)) final_isotopes_peaklist_AsVector[index_vectorize]<-paste(final_isotopes_peaklist$mass[index_vectorize],final_isotopes_peaklist$intensity[index_vectorize],final_isotopes_peaklist$rt[index_vectorize])
          final_isotopes_peaklist_AsVector<-paste0(final_isotopes_peaklist_AsVector, collapse="\n")
          final_isotopes_peaklist_AsVector<-paste0("Isotopes:",final_isotopes_peaklist_AsVector,collapse="")
        }
        #final_isotopes_peaklist_AsVector
        
        if(length(final_adducts)>0){
          final_adducts_peaklist<-peaklist[final_adducts,]
          final_adducts_peaklist<-final_adducts_peaklist[!duplicated(final_adducts_peaklist),]
          final_adducts_peaklist<-final_adducts_peaklist[order(final_adducts_peaklist$mass),]
          row.names(final_adducts_peaklist)<-NULL
          final_adducts_peaklist$intensity<-round(final_adducts_peaklist$intensity,0)
          final_adducts_peaklist$rt<-round(final_adducts_peaklist$rt,3)
          
          index_vectorize<-1; final_adducts_peaklist_AsVector<-c()
          for(index_vectorize in 1:nrow(final_adducts_peaklist)) final_adducts_peaklist_AsVector[index_vectorize]<-paste(final_adducts_peaklist$mass[index_vectorize],final_adducts_peaklist$intensity[index_vectorize],final_adducts_peaklist$rt[index_vectorize])
          final_adducts_peaklist_AsVector<-paste0(final_adducts_peaklist_AsVector, collapse="\n")
          final_adducts_peaklist_AsVector<-paste0("Adducts:",final_adducts_peaklist_AsVector,collapse="")
        }
        #final_adducts_peaklist_AsVector
        
        if(length(final_isotopes)>0 & length(final_adducts)>0) componentinformation<-paste0(final_isotopes_peaklist_AsVector,"\n",final_adducts_peaklist_AsVector)
        else if(length(final_isotopes)==0 & length(final_adducts)>0) componentinformation<-paste0(final_adducts_peaklist_AsVector)
        else if(length(final_isotopes)>0 & length(final_adducts)==0) componentinformation<-paste0(final_isotopes_peaklist_AsVector)
        else if(length(final_isotopes)==0 & length(final_adducts)==0) componentinformation<-"-"
        
        temp$componentinformation<-componentinformation
        
        if(length(final_isotopes)>0){
          temp$isotopicpeaks[1]<-paste0(isotopes_temp,collapse=",")
        } else {
          temp$isotopicpeaks[1]<-"-"
        }
        
        if(length(final_adducts)>0){
          temp$adductpeaks[1]<-paste0(adducts_temp,collapse=",")
        } else {
          temp$adductpeaks[1]<-"-"
        }
        
        output2[i,]<-temp
        output0[i,]<-0
        
      }
    } else {
      #############Case of a previous elimination------
      output2[i,]<-output0[i,]
      output0[i,]<-0
    }
    i<-i+1
    # print(sum(output0$mz))
    print(paste("Processing peak:", i, "of", length(output0$mz)))
  }
  
  
  output2<-output2[output2$mz!=0,]
  
  
  #Fill in adduct relations detected
  i<-1
  for(i in 1:length(output2$mz)){
    if(output2$adduct[i]=="-"){
      # do nothing
    } else {
      # aa<-unique(strsplit(output2$adduct[i],",")[[1]])
      # aa<-aa[aa!="-"]
      if(input$ionization_type=="positive"){
      output2$adduct[i]<-unique(gsub(x=adduct_POS[[1]]$`adduct(s)`[adduct_POS[[1]]$`m/z`==output2$mz[i] &
                                                                     adduct_POS[[1]]$ret==output2$rt[i] &
                                                                     adduct_POS[[1]]$int==output2$int[i]],
                                     pattern="//",replacement=","))
      } else {
        output2$adduct[i]<-unique(gsub(x=adduct_NEG[[1]]$`adduct(s)`[adduct_NEG[[1]]$`m/z`==output2$mz[i] &
                                                                       adduct_NEG[[1]]$ret==output2$rt[i] &
                                                                       adduct_NEG[[1]]$int==output2$int[i]],
                                       pattern="//",replacement=","))
      }
      
      if(output2$adduct[i]=="none") output2$adduct[i]<-"-"
      
    }
  }
  
  output2->output
  names(output)[1:2]<-c("HI m/z |","HI RT |")
  
  
  path3 <- tryCatch(
    if (!is.na(input$datadependentpositive) && !is.null(input$datadependentpositive_local) && exists("mzML_folder"))
      paste0(mzML_folder, input$datadependentpositive_local)
    else "",
    error = function(e) ""
  )
  # Only attempt MS/MS enrichment when the raw mzML file actually exists on
  # disk.  In the automated server pipeline the files are never present in the
  # container, so this block is always skipped.
  if(!is.na(input$datadependentpositive) && nchar(path3) > 0 && file.exists(path3)){
    #incProgress(0.1/2, detail = paste("Adding MS/MS spectra"))
    
    information<-mzR::header(mzR::openMSfile(path3))
    file_spectrum<-RMassBank::makePeaksCache(msRaw=mzR::openMSfile(path3),
                                             headerCache=mzR::header(object=mzR::openMSfile(path3)))
    
    j<-1; 
    output$msms2<-c(0);
    for(j in 1:dim(output)[1]){
      filter1<-information[which(abs(c(output$"HI RT |"[j])*60-information$retentionTime)<20*2),] #Retention time filter. MS/MS should  be Â± 20 sec from top of the peak (I assume peakwidth 40 secs)
      filter2<-filter1[which(abs(output$"HI m/z |"[j]-filter1$precursorMZ)<0.05*2),] #Mass filter Â± 0.05 Da. Obviously very generous with mass error, but there is reason. Precursor mz has huge uncertainty in some HRMS.
      filter3<-filter2[which.max(filter2$peaksCount),]
      
      if(dim(filter3)[1]==1){ #This will remove the selected scans (passed RT and mz filter) from information. These MS/MS scans should not belong to other MS1 peak.
        #print(j)
        # o<-1
        # for(o in 1:dim(filter2)[1]) information<-information[-c(which(information$acquisitionNum==filter2$acquisitionNum[o])),]
        
        spectra<-as.data.frame(file_spectrum[[filter3$seqNum]])
        spectra<-spectra[spectra$V2>input$electronic_noise_cutoff_POS,]
        if(dim(spectra)[1]>0){
          automsms<-paste(round(spectra$V1,5),round(spectra$V2,0),sep=" ",collapse=",")
          
          if(output$componentinformation[j]=="-"){
            output$msms2[j]<-paste0("HRMS/MS:",automsms,  collapse="\n")
          } else {
            output$msms2[j]<-paste0(output$componentinformation[j],"\nHRMS/MS:",automsms,  collapse="\n")
          }
          
        } else {
          output$msms2[j]<-output$componentinformation[j]
        }
        
      } else {
        output$msms2[j]<-output$componentinformation[j]
      }
    }
    
  } else {
    #incProgress(0.1/2, detail = paste("By-pass addition of MS/MS spectra"))
    output$msms2<-output$componentinformation
  }
  #End of new code for improving component information.
  
  formatedoutput<-data.frame(sampleid=paste0(input$Short_Name,"_",input$unique_name),
                             rt=rep(0,length(output$`HI RT |`)),
                             rt2=rep("-",length(output$`HI RT |`)),
                             mz=rep(0,length(output$`HI RT |`)),
                             int=rep(0,length(output$`HI RT |`)),
                             intblank=rep("-",length(output$`HI RT |`)),
                             adduct=rep(0,length(output$`HI RT |`)), 
                             other=rep("-",length(output$`HI RT |`)), 
                             availablemsms=rep(0,length(output$`HI RT |`)),
                             category=rep("-",length(output$`HI RT |`)),
                             name=rep("",length(output$`HI RT |`)),
                             molformula=rep("",length(output$`HI RT |`)),
                             dummycolumn=rep("",length(output$`HI RT |`)),
                             exact_mass=rep("",length(output$`HI RT |`)),
                             smiles=rep("",length(output$`HI RT |`)),
                             cas=rep("",length(output$`HI RT |`)),
                             concentration=rep("",length(output$`HI RT |`)),
                             level=rep("",length(output$`HI RT |`)),
                             msms=rep("-",length(output$`HI RT |`)),
                             rti_tum=rep("",length(output$`HI RT |`)),
                             rti_uoa=rep("",length(output$`HI RT |`)),
                             ri=rep("",length(output$`HI RT |`)),
                             samplingdate=rep("",length(output$`HI RT |`)),
                             samplinganalysis=rep("",length(output$`HI RT |`)),
                             serial=rep("LC001",length(output$`HI RT |`)),
                             numpeaks=rep("",length(output$`HI RT |`)),
                             cutoff=rep("",length(output$`HI RT |`)),
                             mzmls=rep("",length(output$`HI RT |`)),
                             aquisition=rep("",length(output$`HI RT |`)),
                             gc="-"
  )
  
  formatedoutput$rt<-output$`HI RT |`
  formatedoutput$mz<-output$`HI m/z |`
  formatedoutput$int<-output$int
  formatedoutput$adduct<-output$adduct
  if(!is.null(input$datadependentpositive)){
    formatedoutput$msms<-output$msms2
  } else {
    formatedoutput$msms<-output$msms2
  }
  
  
  i<-1
  for(i in 1:length(formatedoutput$availablemsms)){
    if(formatedoutput$msms[i]=="0" || is.null(formatedoutput$msms[i]) || formatedoutput$msms[i]=="" || !grepl(x=formatedoutput$msms[i], pattern="HRMS/MS")) formatedoutput$availablemsms[i]<-"No"
    else formatedoutput$availablemsms[i]<-"Yes"
  }
  
  a<-1
  for(a in 1:length(formatedoutput$availablemsms)){
    if(formatedoutput$msms[a]=="0" || is.null(formatedoutput$msms[i])){
      #print(a)
      formatedoutput$msms[a]<-"-"
    }
  }  
  
  formatedoutput$cutoff<-input$electronic_noise_cutoff_POS
  formatedoutput$numpeaks<-length(formatedoutput$rt)
  
  
  
  assign("formatedoutput",formatedoutput,.GlobalEnv)
  
  
  #Names of mzML files and 
  institutename<-input$institutename
  instrumentname<-input$instrument_type
  
  xlsxname<-paste0(institutename,"_",
                   instrumentname,"_",
                   input$Short_Name,"_",
                   input$City,"_",
                   input$Country_wheresamplecomesfrom,"_",
                   if(is.na(input$"Sampling date")) NA
                   else format(as.Date(trimws(unlist(strsplit(unlist(strsplit(input$"Sampling date",","))[2],"-"))[1], "both"), 
                                       tryFormats = c("%m/%d/%Y")), format="%d.%m.%Y")
                   
                   ,"_",
                   input$Title_Project,"_",
                   input$unique_name)
  assign("xlsxname",xlsxname,.GlobalEnv)
  
  ExtensionFullscanspositive<-strsplit(input$fullscanpositive_local[1][[1]],"\\.")[[1]][length(strsplit(input$fullscanpositive_local[1][[1]],"\\.")[[1]])]
  mzMLname_fullscan<-paste0(institutename,"_",
                            "POS","_",
                            "4","_",
                            instrumentname,"_",
                            input$Short_Name,"_",
                            input$City,"_",
                            input$Country_wheresamplecomesfrom,"_",
                            if(is.na(input$"Sampling date")) NA
                            else format(as.Date(trimws(unlist(strsplit(unlist(strsplit(input$"Sampling date",","))[2],"-"))[1], "both"), 
                                                tryFormats = c("%m/%d/%Y")), format="%d.%m.%Y"),"_",
                            input$Title_Project,"_",
                            input$unique_name,".",ExtensionFullscanspositive)
  assign("mzMLname_fullscan",mzMLname_fullscan,.GlobalEnv)
  
  
  if(!is.null(input$howmanychannelsinpositiveionization)){
    if(input$howmanychannelsinpositiveionization>0){
      i<-1
      CEofchannelspositive<-unlist(lapply(1:input$howmanychannelsinpositiveionization, function(i){ input[[paste0("DataIndependent_channel",i)]] }))
      FilesCEofchannelspositive<-unlist(lapply(1:input$howmanychannelsinpositiveionization, function(i){ input[[paste0("dataindependentpositive_local",i)]][1] }))
      
      ExtensionFilesCEofchannelspositive<-unlist(lapply(1:length(FilesCEofchannelspositive), 
                                                        function(i){ strsplit(FilesCEofchannelspositive[[i]],"\\.")[[1]][length(unlist(strsplit(FilesCEofchannelspositive[[i]],"\\.")))] }))
      mzMLnames_dataindependent<-paste0(institutename,"_",
                                        "POS","_",
                                        CEofchannelspositive,"_",
                                        instrumentname,"_",
                                        input$Short_Name,"_",
                                        input$City,"_",
                                        input$Country_wheresamplecomesfrom,"_",
                                        if(is.na(input$"Sampling date")) NA
                                        else format(as.Date(trimws(unlist(strsplit(unlist(strsplit(input$"Sampling date",","))[2],"-"))[1], "both"), 
                                                            tryFormats = c("%m/%d/%Y")), format="%d.%m.%Y"),"_",
                                        input$Title_Project,"_",
                                        input$unique_name,".",ExtensionFilesCEofchannelspositive)
      assign("mzMLnames_dataindependent",mzMLnames_dataindependent,.GlobalEnv)
    } else {
      mzMLnames_dataindependent<-NA
    }
  } else {
    mzMLnames_dataindependent<-NA
  }
  
  if(input$doyouhavedatadependentpositivetosubmit=="Yes" & !is.null(input$datadependentpositive_local)){
    ExtensionDataDependentpositive<-strsplit(input$datadependentpositive_local[1][[1]],"\\.")[[1]][length(strsplit(input$datadependentpositive_local[1][[1]],"\\.")[[1]])]
    mzMLname_datadependent<-paste0(institutename,"_",
                                   "POS","_",
                                   "DataDependent","_",
                                   instrumentname,"_",
                                   input$Short_Name,"_",
                                   input$City,"_",
                                   input$Country_wheresamplecomesfrom,"_",
                                   if(is.na(input$"Sampling date")) NA
                                   else format(as.Date(trimws(unlist(strsplit(unlist(strsplit(input$"Sampling date",","))[2],"-"))[1], "both"), 
                                                       tryFormats = c("%m/%d/%Y")), format="%d.%m.%Y")
                                   ,"_",
                                   input$Title_Project,"_",
                                   input$unique_name,".",ExtensionDataDependentpositive)
    assign("mzMLname_datadependent",mzMLname_datadependent,.GlobalEnv)
  } else {
    mzMLname_datadependent<-NA
  }
  
  if(is.na(mzMLnames_dataindependent[1]) & is.na(mzMLname_datadependent)){
    formatedoutput$mzmls<-paste(mzMLname_fullscan)
    formatedoutput$aquisition<-paste("Full Scan MS")
  } else if(is.na(mzMLnames_dataindependent[1]) & !is.na(mzMLname_datadependent)){
    formatedoutput$mzmls<-paste(mzMLname_fullscan, mzMLname_datadependent,sep=";")
    formatedoutput$aquisition<-paste("Full Scan MS","Data dependent",sep=";")
  } else if(!is.na(mzMLnames_dataindependent[1]) & is.na(mzMLname_datadependent)){
    formatedoutput$mzmls<-paste(mzMLname_fullscan,paste(mzMLnames_dataindependent, collapse=";"),sep=";")
    formatedoutput$aquisition<-paste("Full Scan MS",paste(mzMLnames_dataindependent, collapse=";"),sep=";")
  } else if(!is.na(mzMLnames_dataindependent[1]) & !is.na(mzMLname_datadependent)){
    formatedoutput$mzmls<-paste(mzMLname_fullscan,paste(mzMLnames_dataindependent, collapse=";"),mzMLname_datadependent,sep=";")
    formatedoutput$aquisition<-paste("Full Scan MS",paste(rep("Data independent",times=length(mzMLnames_dataindependent)),collapse=";"),"Data dependent",sep=";")
  }
  
  #incProgress(0.05/2, detail = paste("Adding RTI"))  
  
  if(is.na(input$"Sampling date")){
    formatedoutput$samplingdate<-NA
  } else {
    formatedoutput$samplingdate<-format(as.Date(trimws(unlist(strsplit(unlist(strsplit(input$"Sampling date",","))[2],"-"))[1], "both"), 
                                                tryFormats = c("%m/%d/%Y")), format="%d.%m.%Y")
  }
  
  if(is.na(input$"Analysis date")){
    formatedoutput$samplinganalysis<-NA
  } else {
    formatedoutput$samplinganalysis<-format(as.Date(trimws(unlist(strsplit(unlist(strsplit(input$"Analysis date",","))[2],"-"))[1], "both"), 
                                                    tryFormats = c("%m/%d/%Y")), format="%d.%m.%Y")
  }
  
  
  if(!input$ihavehistoricaldata){
    RTI_pos<-rti_calibrants2
    RTI_pos<-RTI_pos[!is.na(RTI_pos$RT),] #Remove calibrants with non-reported RT
    RTI_pos$RT <- as.numeric(RTI_pos$RT)
    RTI_pos<-RTI_pos[order(RTI_pos$RT,decreasing = F),] #Orber based on reported RT
    RTI_pos$RTI<-c(1,rep(NA,times=c(dim(RTI_pos)[1]-2)),1000) #Add column RTI. First RT will be 1 and last 1000
    index<-2
    for(index in 1:dim(RTI_pos)[1]){
      if(is.na(RTI_pos$RTI[index])){
        RTI_pos$RTI[index]<-c(as.numeric(RTI_pos$RT[index])-min(as.numeric(RTI_pos$RT),na.rm=TRUE))/c(max(as.numeric(RTI_pos$RT), na.rm=TRUE)-min(as.numeric(RTI_pos$RT), na.rm=TRUE))*1000
      }
    }
    linearmodel<-lm(as.numeric(RTI)~as.numeric(RT), data=RTI_pos)
    
    formatedoutput$rti_uoa<-NA
    dataframeforprediction<-as.data.frame(formatedoutput$rt)
    names(dataframeforprediction)<-"RT"
    formatedoutput$rti_uoa<-predict(linearmodel, newdata=dataframeforprediction)
  }
  assign("formatedoutput",formatedoutput,.GlobalEnv)
  
  
  
  
  
  
  #Add peak area to the formatedoutput object
  temp_peaklist2 <- .xset_peaks_area_df  # pre-extracted before rm(xset) above
  peaklist2 <- data.frame(mass      = temp_peaklist2$mz,
                           intensity = temp_peaklist2$maxo,
                           rt        = round(temp_peaklist2$rt / 60, 3),
                           area      = temp_peaklist2$into)
  t<-1; formatedoutput$area<-NA
  for(t in 1:dim(formatedoutput)[1]){
    selector<-intersect(intersect(
      which(formatedoutput$int[t]==peaklist2$intensity), 
      which(formatedoutput$mz[t]==peaklist2$mass)),
      which(formatedoutput$rt[t]==peaklist2$rt))
    
    if(length(selector)>1) selector<-selector[1]
    
    formatedoutput$area[t]<-peaklist2$area[selector]
  }
  #incProgress(0.1, detail = paste("Generating excel file"))  
  ##########################Save RData files##########################
  names(formatedoutput)<-c("Sample identification (link to the raw data file name)","Retention time in     the 1st column     [min]","Retention time in the 2nd column     [sec]","Mass of ion [m/z] (peak or component)","Intensity of the ion"," Intensity of the ion in the blank","Ion type","Other","MS/MS available","Category","Proposed identification (name of the substance or n.i. for not identified)","Molecular formula","SMILES","Exact. Mass","Identifier: SMILES","CAS No.","Estimated concentration  [ug/l]","Level of confirmation of identification ","Component information","Retention Time  Index LC-MS (Letzel's index; other index)","Retention Time Index LC-MS (UoA approach)","Retention Time Index GC-MS (Kovat's index)","Date of sampling (DD/MM/YYYYY)","Date of analysis (DD/MM/YYYYY)","Serial No. in Method LC-MS(MS) or GC-MS(MS) worksheet","No. of peaks","Intensity cut-off value","Data analysis report (mzML/mzXML)","Data aquisition","GC-MS - files attached","Peak area")
  assign(paste0(xlsxname),formatedoutput)
  # to_be_saved_obj = paste(xlsxname, sep = "")
  # save(list = to_be_saved_obj, file = paste0("componentization output/",xlsxname,".RData"))
  # write.csv(formatedoutput, file = paste0("componentization output/",xlsxname,".csv"))
  
  #Peak picking for data independent files
  if(!is.null(input$howmanychannelsinpositiveionization)){
    if(input$howmanychannelsinpositiveionization>0){
      Paths_DataIndependentFiles_POS<-paste0(mzML_folder,unlist(lapply(1:input$howmanychannelsinpositiveionization, function(i){ input[[paste0("dataindependentpositive_local",i)]] })))
      assign("Paths_DataIndependentFiles_POS",Paths_DataIndependentFiles_POS,.GlobalEnv)
      
      fragmentpeaklists<-list()
      chrom<-1
      for(chrom in 1:input$howmanychannelsinpositiveionization){
        #incProgress(scales::rescale(1:input$howmanychannelsinpositiveionization)/10/2, detail = paste("Peak-picking of data-independent"))
        xset2 <-try(xcms::xcmsSet(Paths_DataIndependentFiles_POS[chrom],
                            method = 'centWave',
                            ppm = input$ppm_positive,
                            prefilter = input$prefilter,
                            peakwidth = input$peakwidth_positive
        ), silent=TRUE)
        
        if(class(xset2)[1]=="try-error"){
          xset2 <-try(xcms::xcmsSet(Paths_DataIndependentFiles_POS[chrom],
                              method = 'centWave',
                              ppm = input$ppm_positive,
                              mslevel=2,
                              prefilter = input$prefilter,
                              peakwidth = input$peakwidth_positive
          ), silent=TRUE)
        }
        
        
        tmp_peaklist<-as.data.frame(xset2@peaks)[,-c(2,3,5,6,7,8,10,11,12)]
        peaklist_MSe<-as.data.frame(cbind(mass=tmp_peaklist[,1],intensity=tmp_peaklist[,3],rt=tmp_peaklist[,2]/60))
        peaklist_MSe$rt<-round(peaklist_MSe$rt,3)
        peaklist_MSe$mass<-round(peaklist_MSe$mass,5)
        peaklist_MSe$file<-input[[paste0("Paths_DataIndependentFiles_POS",chrom)]]
        fragmentpeaklists[[chrom]]<-peaklist_MSe
      }
      assign("fragmentpeaklists",fragmentpeaklists,.GlobalEnv)
      
      #Save fragment RData
      fragmentpeaklists2<-do.call(rbind.data.frame,fragmentpeaklists)
      fragmentpeaklists3<-fragmentpeaklists2[,c(1,3,2,4)]
      names(fragmentpeaklists3)<-c("Mass of ion [m/z]", "Retention time [min]", "Intensity of the ion", "File")
      #assign("fragmentpeaklists3",fragmentpeaklists3,.GlobalEnv) #Fragment list POS
      
      insourcefragments_peaklist <- .xset_peaks_insource_df  # pre-extracted before rm(xset) above
      insourcefragments_peaklist$File<-mzMLname_fullscan
      names(insourcefragments_peaklist)<-c("Mass of ion [m/z]", "Retention time [min]", "Intensity of the ion", "File")
      insourcefragments_peaklist$`Retention time [min]`<-round(insourcefragments_peaklist$`Retention time [min]`/60,3)
      #assign("insourcefragments_peaklist",insourcefragments_peaklist,.GlobalEnv) #MS list POS
      
      fragmentpeaklists4<-data.frame(a=c(insourcefragments_peaklist$`Mass of ion [m/z]`,fragmentpeaklists3$`Mass of ion [m/z]`),
                                     b=c(insourcefragments_peaklist$`Retention time [min]`,fragmentpeaklists3$`Retention time [min]`),
                                     c=c(insourcefragments_peaklist$`Intensity of the ion`,fragmentpeaklists3$`Intensity of the ion`),
                                     d=c(insourcefragments_peaklist$File,fragmentpeaklists3$File))
      # fragmentpeaklists4<-rbind.data.frame(insourcefragments_peaklist,fragmentpeaklists3) #POS together
      names(fragmentpeaklists4)<-c("Mass of ion [m/z]", "Retention time [min]", "Intensity of the ion", "File")
      assign("fragmentpeaklists4",fragmentpeaklists4,.GlobalEnv) #MS list POS
      
      finallist<-data.frame(a=c(fragmentpeaklists4$`Mass of ion [m/z]`),
                            b=c(fragmentpeaklists4$`Retention time [min]`),
                            c=c(fragmentpeaklists4$`Intensity of the ion`),
                            d=c(as.character(fragmentpeaklists4$File)))
      finallist$d<-as.character(finallist$d)
      names(finallist)<-c("Mass of ion [m/z]", "Retention time [min]", "Intensity of the ion", "File")
      assign(paste0(xlsxname,"_Fragments"),finallist)
      # to_be_saved_obj2 = paste0(xlsxname,"_Fragments")
      # save(list = to_be_saved_obj2, file = paste0("componentization output/",xlsxname,"_Fragments.RData"))
      # write.csv(finallist, file = paste0("componentization output/",xlsxname,"_Fragments.RData.csv"))
      
    } else {
      finallist<-data.frame("Mass of ion [m/z]"=0, "Retention time [min]"=0, "Intensity of the ion"=0, "File"="a")
      finallist<-finallist[-1,]
    }
  } else {
    finallist<-data.frame("Mass of ion [m/z]"=0, "Retention time [min]"=0, "Intensity of the ion"=0, "File"="a")
    finallist<-finallist[-1,]
  }
  
    # Write the output to local storage. Files will be uploaded to S3 later
    # during the "Prepare" step.
    local_output_dir <- paste0("/data/index/", files$sample_id[w])
    dir.create(local_output_dir, recursive = TRUE, showWarnings = FALSE)
    local_output_path <- paste0(local_output_dir, "/", files$sample_id[w], "-componentize.RData")
    
    save(samples, files, instrument_setups, input, instrument, formatedoutput, finallist,
         file = local_output_path)
    cat("Saved componentize output to:", local_output_path, "\n")


}

# Entry point: process the single sample passed on the command line.
componentize(collection_id = .cli_args[[1]], sample_id = .cli_args[[2]])





