#' @title Heurystyczna kontrola kompletnosci danych wejsciowych
#' @description
#' Funkcja przygotowuje zestawienie liczby wierszy w poszczególnych plikach
#' z tabelami *wejściowymi* w podanych katalogach.
#' @param ... ciągi znaków ze ścieżkami do innych, porównywanych katalogów
#' @returns Ramka danych z wierszami reprezentującymi poszczególne tabele
#' *wejściowe* i kolumnami reprezentującymi poszczególne katalogi.
#' @examples
#' \dontrun{
#'   w <- porownaj_liczbe_wierszy_w_tabelach_wejsciowych("./",
#'                                                       "../"dane 2024)
#'   print(w, n = Inf)
#' }
#' @importFrom dplyr %>% bind_rows
#' @importFrom tidyr pivot_wider
#' @export
porownaj_liczbe_wierszy_w_tabelach_wejsciowych <- function(...) {
  katalogi <- as.list(...)
  names(katalogi) <- katalogi

  tabeleWejsciowe <- names(tabele_wejsciowe())
  katalogi <-
    lapply(katalogi,
           function(folder, tabeleWejsciowe) {
             pliki <- setNames(paste0(sub("[\\/]$", "", folder), "/",
                                      tabeleWejsciowe, ".csv"),
                               tabeleWejsciowe)
             pliki <- pliki[file.exists(pliki)]
             pliki <- sapply(pliki,
                             function(x)
                             {return(nrow(
                               arrow::read_delim_arrow(x, delim = ";")))})
             return(data.frame(Tabela = names(pliki),
                               liczbaWierszy = pliki))
           }, tabeleWejsciowe = tabeleWejsciowe) %>%
    bind_rows(.id = "katalog") %>%
    pivot_wider(names_from = "katalog",
                values_from = "liczbaWierszy", values_fill = 0L)
  return(katalogi)
}
