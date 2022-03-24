


# inputs for reading table 46 supplement
inputs_tab46_suppl <- list(
  agri = list(
    sheet = 1, start = "c"
  ),
  biology = list(
    sheet = 1, start = "f"
  ),
  health = list(
    sheet = 1, start = "i"
  ),
  chem = list(
    sheet = 2, start = "c"
  ),
  geo = list(
    sheet = 2, start = "f"
  ),
  phys = list(
    sheet = 2, start = "i"
  ),
  cs = list(
    sheet = 3, start = "c"
  ),
  math = list(
    sheet = 3, start = "f"
  ),
  engn = list(
    sheet = 4, start = "c"
  ),
  psych = list(
    sheet = 5, start = "c"
  ),
  econ = list(
    sheet = 5, start = "f"
  ),
  polisci = list(
    sheet = 5, start = "i"
  ),
  soc = list(
    sheet = 5, start = "l"
  ),
  other_socsci = list(
    sheet = 5, start = "o"
  ),
  educ = list(
    sheet = 6, start = "c"
  ),
  hum_art = list(
    sheet = 6, start = "f"
  ),
  other = list(
    sheet = 6, start = "i"
  )
)



read_field <- function(fn, specs) {
  #Read data for specific field from table 46 supplement file `fn`.
    # `specs` is a named list of lists with elements `sheet` and `start`, indicating
    # where in `fn` to read the data from. 
  
  # starting rows for each variable
  startrow_var = list(
    total_count = 8,
    academia = 18,
    government = 28,
    business = 38
  )
  
  years <- read_excel(fn,
                      sheet = 1,
                      range = anchored("A8", dim = c(9, 1)),
                      col_names = c("year")
  )
  
  # loop over variables
  out <- lapply(names(startrow_var), function(x){
    anchor_start = paste0(specs[["start"]],
                          startrow_var[[x]])

    df <- read_excel(fn, sheet = specs[["sheet"]],
                     range = anchored(anchor_start, dim = c(9, 2)),
                     col_names = c("male", "female")) %>%
      mutate(across(.fns = as.numeric))
    
    df <- bind_cols(years, df) %>%
      mutate(var = x)
    
    return(df)
  })
  
  out <- bind_rows(out) 
  return(out)
  
}