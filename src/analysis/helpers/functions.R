


quibble <- function(x, q = c(0.25, 0.5, 0.75), na_action = FALSE, return_qvar = FALSE) {
  #' Summarise a vector at multiple quantiles
  #' 
  #' @param x a numerical vector to be summarise
  #' @param q vector of quantiles to calculate. Defaults to `c(0.25, 0.5, 0.75)`
  #' @param na_action should NA values be ignored? Default is False.
  #' @param return_qvar should the vector `q` with the quantiles be returned? Default is False.

  
  stopifnot(min(q) >= 0 & max(q) <= 1)
  if (return_qvar) {
    tibble("{{ x }}" := quantile(x, q, na.rm = na_action), "{{ x }}_q" := q)
  }
  else {
    tibble("{{ x }}" := quantile(x, q, na.rm = na_action))
  }
}


prep_xvars <- function(x, transform = NULL, rescale = F, recenter = F) {
  #' Prepare explanatory variables for research output
  #' 
  #' @param x a numerical vector
  #' @param transform should `x` be transformed? Default is NULL. Options are `log` or `asinh`, in which case the respective functions are applied to `x`
  #' @param rescale should `x` be rescaled to have standard deviation 1?
  #' @param recenter should `x be recentered to have mean 0?`
  #' 
  #' @return prepped `x`
  
  if (is.null(transform)) {
    x <- x
  } else {
    stopifnot(transform %in% c("log", "asinh"))
    if (transform == "log") {
      x <- log(x)
    }
    else if (transform == "asinh") {
      x <- asinh(x)
    }
  }

  # standardize the output measures for easier interpretation of the female dummy
  if (any(rescale, recenter)) {
    x <- as.vector(scale(x, center = recenter, scale = rescale))
  }
  
  return(x)
}

# rank units according to quartiles of x 
define_rank <- function(x) {
  #' Define the quartile in the vector x.
  #' 
  #' @param x a numerical vector
  #' ``
  #' @return quartile of `x`

  out <- case_when(
    x <= quantile(x, 0.25) ~ 1,
    x <= quantile(x, 0.5) ~ 2,
    x <= quantile(x, 0.75) ~ 3,
    TRUE ~ 4
  )
  return(out)
}

