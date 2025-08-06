#' @title Przygotowanie danych do tabeli wejsciowej W26
#' @description Funkcja przygotowuje zestawienie unikalnych wartości id RSPO
#' szkoły i roku szkolnego występujące w danych z monitoringu (tj w tabelach
#' *wejściowych* W2, W3, W4 i W5).
#' @param w2 ramka danych: tabela *wejściowa* W2
#' @param w3 ramka danych: tabela *wejściowa* W3
#' @param w4 ramka danych: tabela *wejściowa* W4
#' @param w5 ramka danych: tabela *wejściowa* W5
#' @param rokMonitoringu rok (edycja) prowadzenie monitoringu (liczba)
#' @returns ramka danych zawierająca kolumny `ID_SZK` i `ROK_SZK`
#' @seealso [wczytaj_tabele_wejsciowe()], [przygotuj_dane_do_w26()]
#' @importFrom dplyr %>% .data across bind_rows distinct group_by mutate reframe
#'                   rename ungroup
zidentyfikuj_szkolo_lata_w_danych <- function(w2, w3, w4, w5, rokMonitoringu) {
  bind_rows(w2 %>%
              select("ID_SZK", ROK_SZK = "ROK_ABS") %>%
              distinct() %>%
              mutate(ROK_SZK = .data$ROK_SZK - 1L,
                     zrodlo = "W2"),
            w3 %>%
              mutate(zrodlo = "W3",
                     across(starts_with("data"),
                            ~data2rokszk(., max = rokMonitoringu - 1L,
                                         tekst = FALSE))) %>%
              rename(ID_SZK = "ID_SZK_KONT") %>%
              group_by(.data$ID_SZK, .data$zrodlo) %>%
              reframe(ROK_SZK = min(.data$DATA_OD_SZK_KONT):max(.data$DATA_DO_SZK_KONT)),
            w4 %>%
              mutate(zrodlo = "W4",
                     across(starts_with("data"),
                            ~data2rokszk(., max = rokMonitoringu - 1L,
                                         tekst = FALSE))) %>%
              rename(ID_SZK = "ID_SZK_KONT") %>%
              group_by(.data$ID_SZK, .data$zrodlo) %>%
              reframe(ROK_SZK = min(.data$DATA_OD_KKZ):max(.data$DATA_DO_KKZ)),
            w5 %>%
              mutate(zrodlo = "W5",
                     across(starts_with("data"),
                            ~data2rokszk(., max = rokMonitoringu - 1L,
                                         tekst = FALSE))) %>%
              rename(ID_SZK = "ID_SZK_KONT") %>%
              group_by(.data$ID_SZK, .data$zrodlo) %>%
              reframe(ROK_SZK = min(.data$DATA_OD_KUZ):max(.data$DATA_DO_KUZ))) %>%
    group_by(.data$ID_SZK, .data$ROK_SZK) %>%
    mutate(zrodlo = paste(.data$zrodlo, collapse = ", ")) %>%
    ungroup() %>%
    distinct() %>%
    mutate(ROK_SZK = paste0(.data$ROK_SZK, "/", .data$ROK_SZK + 1L)) %>%
    return()
}
