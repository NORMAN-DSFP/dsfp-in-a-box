#* @post /semiquantification
#* @param collection_id
#* @param sample_id
#* @param SMILES_suspect
#* @param Area_suspect
#* @param Preconcentration
function(collection_id, sample_id, SMILES_suspect, Area_suspect, Preconcentration){
  library("readr")
  library("ChemmineR")
  library("rjson")
  Sys.setenv(VROOM_CONNECTION_SIZE = 1000000)
  #collection_id <- 1 #dummy number
  #sample_id<-21537
  #SMILES_suspect <-"CCNC1=NC(=NC(=N1)Cl)NC(C)C"
  #Area_suspect <- 135020
  #Preconcentration <- 1
  
  sample_id_link<-paste0("http://dsfp.norman-data.eu/data/", sample_id, "/spiked-compounds.csv")
  
  spiked_compounds <- read_delim(sample_id_link, "\t", escape_double = FALSE, skip = 1)

  spiked_compounds<-names(spiked_compounds)
  spiked_compounds<-trimws(unlist(strsplit(spiked_compounds,"\n")),"both")
  
  a<-spiked_compounds[c(1,seq(from=27, to=length(spiked_compounds), by=23))]
  b<-spiked_compounds[c(2,seq(from=28, to=length(spiked_compounds), by=23))]
  c<-spiked_compounds[c(3,seq(from=29, to=length(spiked_compounds), by=23))]
  d<-spiked_compounds[c(4,seq(from=30, to=length(spiked_compounds), by=23))] 
  e<-spiked_compounds[c(5,seq(from=31, to=length(spiked_compounds), by=23))] 
  f<-spiked_compounds[c(6,seq(from=32, to=length(spiked_compounds), by=23))] 
  g<-spiked_compounds[c(7,seq(from=33, to=length(spiked_compounds), by=23))] 
  h<-spiked_compounds[c(8,seq(from=34, to=length(spiked_compounds), by=23))] 
  i<-spiked_compounds[c(9,seq(from=35, to=length(spiked_compounds), by=23))] 
  j<-spiked_compounds[c(10,seq(from=36, to=length(spiked_compounds), by=23))] 
  k<-spiked_compounds[c(11,seq(from=37, to=length(spiked_compounds), by=23))] 
  l<-spiked_compounds[c(12,seq(from=38, to=length(spiked_compounds), by=23))] 
  m<-spiked_compounds[c(13,seq(from=39, to=length(spiked_compounds), by=23))] 
  n<-spiked_compounds[c(14,seq(from=40, to=length(spiked_compounds), by=23))] 
  o<-spiked_compounds[c(15,seq(from=41, to=length(spiked_compounds), by=23))] 
  p<-spiked_compounds[c(16,seq(from=42, to=length(spiked_compounds), by=23))] 
  q<-spiked_compounds[c(17,seq(from=43, to=length(spiked_compounds), by=23))] 
  r<-spiked_compounds[c(18,seq(from=44, to=length(spiked_compounds), by=23))] 
  s<-spiked_compounds[c(19,seq(from=45, to=length(spiked_compounds), by=23))] 
  t<-spiked_compounds[c(20,seq(from=46, to=length(spiked_compounds), by=23))] 
  u<-spiked_compounds[c(21,seq(from=47, to=length(spiked_compounds), by=23))] 
  
  
  spikedcompounds<- data.frame(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u)
  
  names(spikedcompounds)<-c("Compound","SMILES","Unit","Response","Concentation level1","Area1","Concentation level2","Area2","Concentation level3","Area3","Concentation level4","Area4","Concentation level5","Area5","Concentation level6","Area6","Concentation level7","Area7","Concentation level8","Area8","RT")
  
  spikedcompounds$"Concentation level1"[spikedcompounds$"Concentation level1"==""]<-NA
  spikedcompounds$"Concentation level1"<-as.numeric(spikedcompounds$"Concentation level1")
  spikedcompounds$Area1[spikedcompounds$Area1==""]<-NA
  spikedcompounds$Area1<-as.numeric(spikedcompounds$Area1)
  
  spikedcompounds$"Concentation level2"[spikedcompounds$"Concentation level2"==""]<-NA
  spikedcompounds$"Concentation level2"<-as.numeric(spikedcompounds$"Concentation level2")
  spikedcompounds$Area2[spikedcompounds$Area2==""]<-NA
  spikedcompounds$Area2<-as.numeric(spikedcompounds$Area2)
  
  spikedcompounds$"Concentation level3"[spikedcompounds$"Concentation level3"==""]<-NA
  spikedcompounds$"Concentation level3"<-as.numeric(spikedcompounds$"Concentation level3")
  spikedcompounds$Area3[spikedcompounds$Area3==""]<-NA
  spikedcompounds$Area3<-as.numeric(spikedcompounds$Area3)
  
  spikedcompounds$"Concentation level4"[spikedcompounds$"Concentation level4"==""]<-NA
  spikedcompounds$"Concentation level4"<-as.numeric(spikedcompounds$"Concentation level4")
  spikedcompounds$Area4[spikedcompounds$Area4==""]<-NA
  spikedcompounds$Area4<-as.numeric(spikedcompounds$Area4)
  
  spikedcompounds$"Concentation level5"[spikedcompounds$"Concentation level5"==""]<-NA
  spikedcompounds$"Concentation level5"<-as.numeric(spikedcompounds$"Concentation level5")
  spikedcompounds$Area5[spikedcompounds$Area5==""]<-NA
  spikedcompounds$Area5<-as.numeric(spikedcompounds$Area5)
  
  spikedcompounds$"Concentation level6"[spikedcompounds$"Concentation level6"==""]<-NA
  spikedcompounds$"Concentation level6"<-as.numeric(spikedcompounds$"Concentation level6")
  spikedcompounds$Area6[spikedcompounds$Area6==""]<-NA
  spikedcompounds$Area6<-as.numeric(spikedcompounds$Area6)
  
  spikedcompounds$"Concentation level7"[spikedcompounds$"Concentation level7"==""]<-NA
  spikedcompounds$"Concentation level7"<-as.numeric(spikedcompounds$"Concentation level7")
  spikedcompounds$Area7[spikedcompounds$Area7==""]<-NA
  spikedcompounds$Area7<-as.numeric(spikedcompounds$Area7)
  
  spikedcompounds$"Concentation level8"[spikedcompounds$"Concentation level8"==""]<-NA
  spikedcompounds$"Concentation level8"<-as.numeric(spikedcompounds$"Concentation level8")
  spikedcompounds$Area8[spikedcompounds$Area8==""]<-NA
  spikedcompounds$Area8<-as.numeric(spikedcompounds$Area8)
  
  spikedcompounds$RT[spikedcompounds$RT==""]<-NA
  spikedcompounds$RT<-as.numeric(spikedcompounds$RT)
  
  #Find the best similarity between the suspected compound and the spiked compound database
  read.SMIset_local<-function (smiles, removespaces = TRUE, ...) {
    smisettmp <- smiles
    if (removespaces == TRUE) 
      smisettmp <- gsub(" {1,}", "", smisettmp)
    index <- !grepl("\t.{1,}$", smisettmp)
    if (any(index)) 
      smisettmp[index] <- paste(smisettmp[index], "\t", "CMP", 
                                which(index), sep = "")
    smiset <- gsub("\t.*$", "", smisettmp)
    names(smiset) <- gsub("^.*\t", "", smisettmp)
    smiset <- as(smiset, "SMIset")
    return(smiset)
  }


  smiset <- read.SMIset_local(smiles=spikedcompounds$SMILES)
  cid(smiset)<-spikedcompounds$SMILES
  cid(smiset) <- makeUnique(cid(smiset))
  apset <- sdf2ap(smiles2sdf(smiset))
  

  similarity<-cmp.search(db=apset,
                         query=sdf2ap(smiles2sdf(SMILES_suspect)),
                         type=3, cutoff = 0.05, quiet=TRUE) 
  assign("similarity",similarity,.GlobalEnv)
  
  #If similar molecule exist in spiked compounds database
  if(nrow(similarity)>0 & !all(is.na(similarity$scores))){
    quantificationcurve<-spikedcompounds[similarity$index[1],]
    concentration<-quantificationcurve[,c(grepl(x=names(quantificationcurve),pattern="Concentation level"))]
    
    response<-quantificationcurve[,c(grepl(x=names(quantificationcurve),pattern="Area"))]
    
    dataset<-data.frame("concentration"=unlist(concentration),"response"=unlist(response))
    dataset<-dataset[!is.na(dataset$concentration),]
    dataset<-dataset[!is.na(dataset$response),]
    assign("dataset",dataset,.GlobalEnv)
    
    linearmodel<-lm(response~concentration, data=dataset)
    lmcoef <- coef(linearmodel)
    
    ynew<-Area_suspect
    assign("ynew",ynew,.GlobalEnv)
    
    num<-1
    for(num in 1:length(ynew)){
      
      if(ynew[num]<=min(dataset$response)){ #if response lower than calibration curve range, quantify based on lower standard
        xnew <- (ynew[num]/min(dataset$response))*min(dataset$concentration)
        
      } else if(ynew[num]>min(dataset$response) & ynew[num]<max(dataset$response)){ #if response in the calibration curve range
        xnew <- (ynew[num]-lmcoef[1])/lmcoef[2]
        
      } else if(ynew[num]>=max(dataset$response)){ #if response higher than calibration curve range quantify based on higher standard
        xnew <- (ynew[num]/max(dataset$response))*max(dataset$concentration)
        
      }
      
      
      
      #Take into account the preconcentration
      # Files$Matrix[which(gsub(x=Files$Name,pattern=".xlsx",replacement="")==names(filteredtable_global_quant)[index2]
      #                                       & Files$Ionization==input$Ionization)]
      
      xnew<-c(xnew)*c(1/Preconcentration)
      
      #if(length(xnew)>0){ 
      #  if(xnew<=0.99) xnew<-round(xnew,2)
      #  else if(xnew>0.99 & xnew<=10) xnew<-round(xnew,1)
      #  else if(xnew>10) xnew<-round(xnew,0)
      #  
      #  filteredtable_global_quant[[index2]]$`Estimated concentration`[num]<-xnew
      #  filteredtable_global_quant[[index2]]$`Estimated concentration unit`[num]<-quantificationcurve$`Concentation level units`
      #  filteredtable_global_quant[[index2]]$`Estimated concentration based on compound`[num]<-quantificationcurve$`Spiked Compound Name`
      #  filteredtable_global_quant[[index2]]$`Estimated concentration based on compound with similarity (%)`[num]<-round(similarity$scores[similarity$index==similarity$index[1]]*100,0)
      #}
    }
  }
  
  
  obj_return<-data.frame("semiqmethod"=c("structural similarity"),
                         "semiqconcentration"=xnew,
                         "semiqbasedon"=similarity$cid[1],
                         "semiqsimilarity"=similarity$scores[1])


  return(toJSON(obj_return))
  
}


#lambdr::start_lambda()

