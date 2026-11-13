#' @title Przygotowanie danych do tabeli wejsciowej W30
#' @description Funkcja nieeksportowana, wykorzystywana przez
#' [przygotuj_dane_do_w30()]: przekształca dane o jednej konkretnej,
#' potencjalnie zmiennej w czasie charakterystyce uczelni (nazwie, organie
#' nadzorującym, statusie, typie lub adresie) z formatu, w którym jest on
#' opisywany datą, od której zaczyna obowiązywać dana wartość charakterystyki do
#' formatu, w którym jednostką obserwacji jest kombinacja zmiennych
#' (`ID_PODMIOTU`, `ROK_AKAD`).
#' @param x ramka danych zawierająca co najmniej kolumny: `ID_PODMIOTU`,
#' `DATA_OD`, `DATA_LIKWIDACJI`, `DATA_MIN` i `DATA_MAX`
#' @inheritParams przygotuj_dane_do_w30 rokMonitoringu
#' @details Jeśli w ciągu danego roku akademickiego obowiązywała więcej niż
#' jedna wartość danej charakterystyki (tj., jej wartość zmieniła się w trakcie
#' roku akademickiego), danemu rokowi akademickiemu przypisywana jest wartość
#' obowiązująca pierwszego dnia tego roku akademickiego.
#' @returns ramka danych zawierająca kolumny `ID_PODMIOTU`, `ROK_AKAD` oraz
#' kolumny opisujące daną charkaterystykę uczelni (lub jej filii)
#' @seealso [wczytaj_tabele_wejsciowe()], [przygotuj_dane_do_w30()]
#' @importFrom dplyr %>% .data across all_of arrange distinct if_else filter group_by lead mutate n reframe select slice_min ungroup
przetworz_czesc_danych_o_uczelni <- function(x, rokMonitoringu) {
  stopifnot(is.numeric(rokMonitoringu), length(rokMonitoringu) == 1L,
            !is.na(rokMonitoringu), rokMonitoringu >= 2021L,
            as.integer(rokMonitoringu) == rokMonitoringu,
            is.data.frame(x),
            all(c("ID_PODMIOTU", "DATA_OD", "DATA_MIN", "DATA_MAX") %in% names(x)),
            "DATA_DO" %in% names(x) | "DATA_LIKWIDACJI" %in% names(x))

  x <- x %>%
    distinct() %>%
    arrange(.data$ID_PODMIOTU, .data$DATA_OD) %>%
    group_by(.data$ID_PODMIOTU) %>%
    mutate(lp = seq_len(n()))
  if (!("DATA_DO" %in% names(x))) {
    x <- x %>%
      mutate(DATA_DO =
               lead(.data$DATA_OD - 1L, default = .data$DATA_LIKWIDACJI[1]))
  }
  x <- x %>%
    ungroup() %>%
    filter(.data$DATA_DO > .data$DATA_MIN,
           .data$DATA_OD < .data$DATA_MAX) %>%
    mutate(DATA_OD = if_else(.data$DATA_OD < .data$DATA_MIN,
                             .data$DATA_MIN, .data$DATA_OD),
           DATA_DO = if_else(.data$DATA_DO > .data$DATA_MAX,
                             .data$DATA_MAX, .data$DATA_DO),
           across(all_of(c("DATA_OD", "DATA_DO")),
                  ~data2rokszk(., max = rokMonitoringu - 1L,
                               akademicki = TRUE, tekst = FALSE))) %>%
    group_by(across(all_of(c(setdiff(names(x),
                                     c("DATA_OD", "DATA_DO", "DATA_LIKWIDACJI",
                                       "DATA_MIN", "DATA_MAX")),
                             "lp")))) %>%
    reframe(ROK_AKAD = .data$DATA_OD:.data$DATA_DO) %>%
    mutate(ROK_AKAD = paste0(.data$ROK_AKAD, "/", .data$ROK_AKAD + 1L)) %>%
    group_by(.data$ID_PODMIOTU, .data$ROK_AKAD) %>%
    slice_min(.data$lp, n = 1) %>%
    ungroup() %>%
    select(-"lp")
  return(x)
}
