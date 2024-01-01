
## Aggregating function to pick "Yes" over "no" responses

aok_yes <- function(x) {
  ux <- unique(x[!is.na(x)])
  # This checks to see if we have more than one mode (a tie), return blank if so.
  if ("yes"%in% x) {
      return("yes") # Blanks are coerced to values in logical vectors, so we specifically identify columns with TRUE/FALSE (KoBo's select multiples) and output a "logical" blank.
    }
    else if("no" %in% x) {
      return("no")
    }

  else {
    return("")
  }

}

## Aggregating function to pick "no" over "yes" responses

aok_no <- function(x) {
  ux <- unique(x[!is.na(x)])
  # This checks to see if we have more than one mode (a tie), return blank if so.
  if ("no"%in% x) {
    return("no") # Blanks are coerced to values in logical vectors, so we specifically identify columns with TRUE/FALSE (KoBo's select multiples) and output a "logical" blank.
  }
  else if("yes" %in% x) {
    return("yes")
  }

  else {
    return("")
  }

}




## Aggregating function to calculate mode, while outputting NC (No consensus) if we don't have a clear winner.

aok_mode <- function(x) {
  ux <- unique(x[!is.na(x)])

  # This checks to see if we have more than one mode (a tie), return blank if so.
  if (length(which(tabulate(match(x, ux)) == max(tabulate(match(x, ux))))) > 1) {
    if (class(x) == "logical") {
      return(as.logical("")) # Blanks are coerced to values in logical vectors, so we specifically identify columns with TRUE/FALSE (KoBo's select multiples) and output a "logical" blank.
    }
    else {
      return("NC")
    }
  }

  else {
    ux[which.max(tabulate(match(x, ux)))] ## This occurs if no tie, so we return the value which has max value! Wambam.
  }

}


AoK <- function(x) {
  aok_mode(x)
}

## Aggregating function to pick most recent responses  over others. For frequency questions

aok_frequency <- function(x) {
  ux <- unique(x[!is.na(x)])
  # This checks to see if we have more than one mode (a tie), return blank if so.
  if ("week"%in% x) {
    return("week")
  }
  else if("two_weeks" %in% x) {
    return("two_weeks")
  }
  else if("one_month" %in% x) {
    return("one_month")
  }
  else if("few_months" %in% x) {
    return("few_months")
  }
  else if("emergency" %in% x) {
    return("emergency")
  }
  else if("dontknow" %in% x) {
    return("dontknow")
  }

  else {
    return("")
  }

}


##Aggregating function to pick most recent responses  over others. For duration questions

aok_recent <- function(x) {
  ux <- unique(x[!is.na(x)])
  # This checks to see if we have more than one mode (a tie), return blank if so.
  if ("3_months"%in% x) {
    return("3_months")
  }
  else if("3_6_months" %in% x) {
    return("3_6_months")
  }
  else if("6_12_months" %in% x) {
    return("6_12_months")
  }
  else if("1_year" %in% x) {
    return("1_year")
  }
  else if("dontknow" %in% x) {
    return("dontknow")
  }

  else {
    return("")
  }

}



##Aggregating function to pick highest responses  over others.

aok_highest <- function(x) {
  ux <- unique(x[!is.na(x)])
  # This checks to see if we have more than one mode (a tie), return blank if so.
  if ("many"%in% x) {
    return("many")
  }
  else if("several" %in% x) {
    return("several")
  }
  else if("some" %in% x) {
    return("some")
  }
  else if("few" %in% x) {
    return("few")
  }
  else if("none" %in% x) {
    return("none")
  }
  else if("dontknow" %in% x) {
    return("dontknow")
  }

  else {
    return("")
  }

}


##Aggregating function to pick conflict rekated responses  over others.

aok_conflict <- function(x) {
  ux <- unique(x[!is.na(x)])
  # This checks to see if we have more than one mode (a tie), return blank if so.
  if ("unsafe"%in% x) {
    return("unsafe")
  }
  else if("crops_destroyed_fight" %in% x) {
    return("crops_destroyed_fight")
  }
  else if(length(which(tabulate(match(x, ux)) == max(tabulate(match(x, ux))))) > 1) {
    if (class(x) == "logical") {
      return(as.logical(""))
    }
    else {
      return("NC")
    }
  }

  else {
    ux[which.max(tabulate(match(x, ux)))] ## This occurs if no tie, so we return the value which has max value! Wambam.
  }

}



