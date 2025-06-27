#' @title Usuwanie zduplikowanych absolwentow z tabel "posrednich"
#' @description Funkcja usuwa zduplikowanych absolwentów - identyfikowanych na
#' podstawie tabeli *pośredniej* P4 - z tabel *pośrednich*.
#' @param x lista tabel pośrednich zwrócona przez [przygotuj_tabele_posrednie()]
#' lub pojedyncza tabela *pośrednia* (tj. ramka danych będąca pojedynczym
#' elementem takiej listy)
#' @param p4 jeśli `x` jest pojedynczą tabelą *pośrednią*, ramka danych
#' z tabelą *pośrednią* P4 (na podstawie której zostaną zidentyfikowane
#' duplikaty); jeśli `x` jest listą, `NULL`
#' @param duplikatWSzkole wartość logiczna - czy usuwać absolwentów, którzy
#' występują kilkakrotnie w ramach tej samej szkoły?
#' @param duplikatWieleSzkol wartość logiczna - czy usuwać absolwentów, którzy
#' występują w kilku różnych szkołach?
#' @details
#' -  *Absolwent* identyfikowany jest przez kombinację wartości kolumn `id_abs`
#' i `rok_abs`.
#' -  Jeśli argumentem `x` przekazywana jest pojedyncza ramka danych z tabelą P4,
#' to należy tę samą ramkę danych przekazać również argumentem `p4`.
#' -  Tabela *pośrednia* **P6** - jeśli jest elementem przekazanej listy - zostanie
#' zwrócona **niezmodyfikowana**.
#' @return lista ramek danych lub ramka danych - w zależności od tego, jaki
#' obiekt został przekazany argumentem `x`
#' @importFrom dplyr %>% anti_join count distinct filter full_join mutate
#'                   n_distinct select semi_join
#' @export
usun_duplikaty <- function(x, p4 = NULL, duplikatWSzkole = TRUE,
                           duplikatWieleSzkol = TRUE) {
  stopifnot(is.logical(duplikatWSzkole), length(duplikatWSzkole) == 1L,
            duplikatWSzkole %in% c(TRUE, FALSE),
            is.logical(duplikatWieleSzkol), length(duplikatWieleSzkol) == 1L,
            duplikatWieleSzkol %in% c(TRUE, FALSE),
            duplikatWSzkole | duplikatWieleSzkol)
  if (is.list(x)) {
    stopifnot("Jeśli argumentem `x` przekazywana jest lista, argument `p4` powinien przyjąć wartość `NULL`." =
                is.null(p4),
              "Lista przekazana argumentem `x` musi zawierać element o nazwie 'p4'." =
                p4 %in% tolower(names(x)),
              "Wszystkie elementy listy przekazanej argumentem `x` muszą być ramkami danych." =
                all(sapply(x, is.data.frame)),
              "Element 'p4' listy przekazanej argumentem `x` musi zawierać kolumny 'id_abs', 'rok_abs' i 'id_szk'." =
                all(c("id_abs", "rok_abs", "id_szk") %in% names(x$p4)),
              "Kolumny 'id_abs', 'rok_abs' i 'id_szk' w elemencie 'p4' listy przekazanej argumentem `x` nie mogą zawierać braków danych." =
                !anyNA(x$p4[, c("id_abs", "rok_abs", "id_szk")]),
              "Wszystkie elementy listy listy przekazanej argumentem `x`, z wyjątkiem `p6`, muszą zawierać kolumny 'id_abs' i 'rok_abs'." =
                all(sapply(x[names(x) != "p6"],
                           function(x) {return(all(c("id_abs", "rok_abs") %in%
                                                     names(x)))})),
              "Wszystkie elementy listy przekazanej argumentem `x`, z wyjątkiem `p6`,  nie mogą zawierać braków danych w kolumnach 'id_abs' ani 'rok_abs'." =
                all(sapply(x[names(x) != "p6"],
                           function(x) {return(!anyNA(x[ c("id_abs", "rok_abs")]))})))
    # to niżej proforma, bo p6 i tak jest potem ignorowana (zwracana bez zmian)
    if ("p6" %in% names(x)) {
      stopifnot(all(c("id_szk", "rok_szk") %in% names(x$p6)),
                !anyNA(x$p6[, c("id_szk", "rok_szk")]),
                !any(duplicated(x$p6[, c("id_szk", "rok_szk")])))
    }
    xLista <- TRUE
    p4 <- x$p4
  } else {
    xLista <- FALSE
    stopifnot(is.data.frame(x), is.data.frame(p4),
              all(c("id_abs", "rok_abs", "id_szk") %in% names(p4)),
              !anyNA(p4[, c("id_abs", "rok_abs", "id_szk")]),
              all(c("id_abs", "rok_abs") %in% names(p4)),
              !anyNA(x[, c("id_abs", "rok_abs")]))
    x <- list(x = x)
  }

  wszystkieDuplikaty <-
    data.frame(id_abs = vector(mode = "integer", length = 0L),
               rok_abs = vector(mode = "integer", length = 0L))
  if (duplikatWSzkole) {
    duplikatWSzkoleTab <- p4 %>%
      count(.data$id_abs, .data$rok_abs, .data$id_szk,
            name = "duplikat_w_szkole") %>%
      mutate(duplikat_w_szkole = .data$duplikat_w_szkole > 1L) %>%
      filter(.data$duplikat_w_szkole)
    if ("duplikat_w_szkole" %in% names(p4)) {
      duplikatWSzkoleX <- p4 %>%
        filter(.data$duplikat_w_szkole) %>%
        select("id_abs", "rok_abs", "id_szk") %>%
        distinct()
      roznica1 <- anti_join(duplikatWSzkoleTab, duplikatWSzkoleX,
                            by = c("id_abs", "rok_abs", "id_szk"))
      roznica2 <- anti_join(duplikatWSzkoleX, duplikatWSzkoleTab,
                            by = c("id_abs", "rok_abs", "id_szk"))
      if (nrow(roznica1) > 0L) {
        warning("Tabela `p4` zawiera już zmienną 'duplikat_w_szkole', ale nie wskazuje ona przynajmniej niektórych absolwentów zduplikowanych w ramach szkół. Sprawdź swoje dane!")
      }
      if (nrow(roznica2) > 0L) {
        warning("Tabela `p4` zawiera już zmienną 'duplikat_w_szkole', ale część absolwentów, których ona identyfikuje, nie jest zduplikowana w ramach szkół (w przekazanej wersji `p4`). Sprawdź swoje dane!")
      }
    }
    wszystkieDuplikaty <- wszystkieDuplikaty %>%
      full_join(duplikatWSzkoleTab, by = c("id_abs", "rok_abs"))
  }
  if (duplikatWieleSzkol) {
    duplikatWieleSzkolTab <- p4 %>%
      select("id_abs", "rok_abs", "id_szk") %>%
      distinct() %>%
      count(.data$id_abs, .data$rok_abs,
            name = "duplikat_wiele_szkol") %>%
      mutate(duplikat_wiele_szkol = .data$duplikat_wiele_szkol > 1L) %>%
      filter(.data$duplikat_wiele_szkol)
    if ("duplikat_wiele_szkol" %in% names(p4)) {
      duplikatWieleSzkolX <- p4 %>%
        filter(.data$duplikat_wiele_szkol) %>%
        select("id_abs", "rok_abs") %>%
        distinct()
      roznica1 <- anti_join(duplikatWieleSzkolTab, duplikatWieleSzkolX,
                            by = c("id_abs", "rok_abs"))
      roznica2 <- anti_join(duplikatWieleSzkolX, duplikatWieleSzkolTab,
                            by = c("id_abs", "rok_abs"))
      if (nrow(roznica1) > 0L) {
        warning("Tabela `p4` zawiera już zmienną 'duplikat_wiele_szkol', ale nie wskazuje ona przynajmniej niektórych absolwentów zduplikowanych pomiędzy szkołami. Sprawdź swoje dane!")
      }
      if (nrow(roznica2) > 0L) {
        warning("Tabela `p4` zawiera już zmienną 'duplikat_wiele_szkol', ale część absolwentów, których ona identyfikuje, nie jest zduplikowana pomiędzy szkołami (w przekazanej wersji `p4`). Sprawdź swoje dane!")
      }
    }
    wszystkieDuplikaty <- wszystkieDuplikaty %>%
      full_join(duplikatWieleSzkolTab, by = c("id_abs", "rok_abs"))
  }
  p4 <- p4 %>%
    anti_join(duplikatWSzkoleTab, by = c("id_abs", "rok_abs", "id_szk"))
  p4 <- p4 %>%
    anti_join(duplikatWieleSzkolTab, by = c("id_abs", "rok_abs"))
  x[names(x) != "p6"] <- lapply(x[names(x) != "p6"],
                                semi_join, y = p4[, c("id_abs", "rok_abs")],
                                by = c("id_abs", "rok_abs"))
  if (nrow(wszystkieDuplikaty) > 0L) {
    wszystkieDuplikaty <- wszystkieDuplikaty %>%
      mutate(abs = paste(.data$id_abs, .data$rok_abs))
    if (!duplikatWSzkole) wszystkieDuplikaty$duplikat_w_szkole = NA
    if (!duplikatWieleSzkol) wszystkieDuplikaty$duplikat_wiele_szkol = NA
    usunieciWSzkole <- n_distinct(wszystkieDuplikaty$abs[
      wszystkieDuplikaty$duplikat_w_szkole %in% TRUE])
    usunieciWieleSzkol <- n_distinct(wszystkieDuplikaty$abs[
      wszystkieDuplikaty$duplikat_wiele_szkol %in% TRUE])
    usunieciOgolem <- n_distinct(wszystkieDuplikaty$abs)
    message(ifelse(xLista, paste0("Z tabel '", paste(setdiff(names(x), "p6"),
                                                     collapse = "', '"),
                                  "' usunięto"),
                   "Usunięto"),
            " dane ", format(usunieciOgolem, big.mark = "'"), " absolwentów (",
            ifelse(duplikatWSzkole,
                   paste("w tym", format(usunieciWSzkole, big.mark = "'"),
                         " spełniających kryterium wielokrotnego występowania w ramach jednej szkoły"),
                   ""),
            ifelse(duplikatWSzkole & duplikatWieleSzkol, " i ", "w tym "),
            ifelse(duplikatWieleSzkol,
                   paste(format(usunieciWieleSzkol, big.mark = "'"),
                         " spełniających kryterium występowania w kilku różnych szkołach"),
                   ""), "), którzy byli reprezentowani przez ",
            format(nrow(wszystkieDuplikaty), big.mark = "'"),
            " rekordów w tabeli 'p4'.")
  } else {
    message("Nie wykryto żadnych duplikatów.")
  }

  if (!xLista) {
    x <- x$x
  }
  return(x)
}
