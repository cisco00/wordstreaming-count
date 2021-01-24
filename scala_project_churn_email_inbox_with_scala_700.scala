
import scala.io.Source
import scala.collection.mutable.Map
import scala.collection.mutable.Map

def number_of_lines(): Int = {
    var a = 0
    val filename = "cxldatasets/datasets/project/mbox-short.txt"
    
    for(line <- Source.fromFile(filename).getLines){
        a = a + 1
    } 
    return (a)
}

def count_subject_lines() : Int = {
    var countLines = 0
    val filename = "cxldata/datasets/project/mbox-short.txt"
    
    for(line <- Source.fromFile(filename).getLines){
        if(line.startsWith("Subject:")){
            countLines = countLines + 1
        }
    }
    return (countLines)
}


def average_spam_confidence() : Float =  {
    val filename = "/cxldata/datasets/project/mbox-short.txt"
    var splittedLineContent = new Array[String](0)
    var spamConf:Float = 0
    var spamCnt:Int = 0

    for (line <- Source.fromFile(filename).getLines) {
        if (line.startsWith("X-DSPAM-Confidence:")) {
            splittedLineContent = line.split(": ")
            spamConf = spamConf + (splittedLineContent(1).toFloat)
            spamCnt = spamCnt + 1
        }
    }
    return(spamConf/spamCnt)
}

def find_email_sent_days() : Map[String, Int] = {
    
   val filepath = "/cxldata/datasets/project/mbox-short.txt"
   var daysMap = Map[String, Int]()
   var splittedLineContent = new Array[String](0)
   var day = ""

   for (line <- Source.fromFile(filepath).getLines) {  
       if (line.startsWith("From")) {
           splittedLineContent = line.split(" ")
           if (splittedLineContent.size > 3) {
               day = splittedLineContent(2)
               if (daysMap.contains(day)) {
                   daysMap(day) += 1
               }
               else {
                   daysMap(day) = 1
               }
           }
       }
   }
  return(daysMap)
}

def count_message_from_email() : Map[String, Int] = {
    val filepath = "/cxldata/datasets/project/mbox-short.txt"
    var emailMap = Map[String,Int]()
    var splittedLineContent = new Array[String](0)
    var email = " "
    
    for(line <- Source.fromFile(filepath).getLines){
        if(line.startsWith("From:")){
            splittedLineContent = line.split("")
            if(emailMap.contains(email)){
                emailMap(email) += 1
            }
            else{
                emailMap(email) = 1
            }
        }
    }
    return (emailMap)
}

def count_message_from_domain() : Map[String, Int] = {
    val filepath = "/cxldata/datasets/project/mbox-short.txt"
    var domainMap = Map[String, Int]()
    var splittedLineContent = new Array[String](0)
    var email = ""
    var splittedEmailContent = new Array[String](0)
    var domain = ""

    for (line <- Source.fromFile(filepath).getLines) {
        if (line.startsWith("From:")) {
            splittedLineContent = line.split(" ")
            email = splittedLineContent(1)
            splittedEmailContent = email.split("@")
            domain = splittedEmailContent(1)
            if (domainMap.contains(domain)) {
                domainMap(domain) += 1
            }
            else {
                domainMap(domain) = 1
            }
        }
    }
    return(domainMap)
}
