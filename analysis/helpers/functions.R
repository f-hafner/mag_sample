

# Function summarise a set of papers in df into equal-sized bins on log10 scale
  # Follows Radicchi et al (2008, PNAS)
prep_hist <- function(df, norm_var, n_breaks = 20) {  
  
  # normalize, calculate bins 
  df <- df %>%
    mutate(cf = cit_measure / .data[[norm_var]], 
           cflog = log10(cf),
           cflog = ifelse(cflog == -Inf, -2, cflog) # this is necessary for papers with 0 citations
           ) 
  
  df <- df %>%
    mutate(grp = cut(cflog, breaks  = n_breaks))
  
  bin_groups <- df %>% 
    filter(!duplicated(paste0(grp, Field1))) %>%
    select(Field1, grp) 
  
  min_max <- unlist(strsplit(gsub("\\(|\\]", "", as.character(bin_groups$grp), perl=TRUE), ","))
  bin_groups$min <- as.numeric(min_max[seq(1, length(min_max), by=2)])
  bin_groups$max <- as.numeric(min_max[seq(2, length(min_max), by=2)])
  
  bin_groups <- bin_groups %>%
    mutate(min_10 = 10^min,
           max_10 = 10^max,
           interval_length = ceiling(max_10) - floor(min_10),
           interval_midpoint = 0.5 * (max_10 + min_10))
  
  # summarise papers by field and bin group
  df <- df %>%
    group_by(grp, Field1) %>%
    summarise(n_articles = n(),
              .groups = "drop") %>%
    left_join(bin_groups %>%
                select(grp, Field1, interval_length, interval_midpoint), 
              by = c("grp", "Field1")) %>%
    mutate(y =  n_articles / interval_length) %>%
    group_by(Field1) %>%
    mutate(n_tot = sum(n_articles)) %>%
    ungroup() 
  
  return(df)
}


# function to prepare explanatory output variables
prep_xvars <- function(x, transform = NULL, rescale = F, recenter = F) {
  
  
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
  # NOTE: this is wrt to the baseline sample; e.g. when using multiple years for the same author,
  # the mean will not be 0 anymore 
  if (any(rescale, recenter)) {
    x <- as.vector(scale(x, center = recenter, scale = rescale))
  }
  
  return(x)
}

# rank units according to quartiles of x 
define_rank <- function(x) {
  out <- case_when(
    x <= quantile(x, 0.25) ~ 1,
    x <= quantile(x, 0.5) ~ 2,
    x <= quantile(x, 0.75) ~ 3,
    TRUE ~ 4
  )
  return(out)
}

