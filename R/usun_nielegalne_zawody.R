#' @title Usuwanie absolwentow w zawodach "nielegalnych" w danym typie szkoly
#' @description Funkcja usuwa z tabel *pośrednich* absolwentów w zawodach, które
#' formalnie (zgodnie z KZSB) nie mogą być nauczane w danym typie szkoły.
#' @param x lista tabel pośrednich zwrócona przez [przygotuj_tabele_posrednie()]
#' lub pojedyncza tabela *pośrednia* (tj. ramka danych będąca pojedynczym
#' elementem takiej listy) lub dowolna ramka danych zawierająca kolumny
#' `typ_szk` i `kod_zaw`
#' @param p4 jeśli `x` jest pojedynczą tabelą *pośrednią*, ramka danych
#' z tabelą *pośrednią* P4 (na podstawie której zostaną zidentyfikowani
#' absolwenci w *nielegalnych* zawodo-typach szkół); jeśli `x` jest listą,
#' lub ramką danych niebędącą tabelą *pośrednią* - `NULL`
#' @param zawodyTypySzkol opcjonalnie ramka danych zawierająca dozwolone
#' kombinacje wartości `kod_zaw`, `typ_szk` (domyślnie użyte zostanie
#' zestawienie zapisane w danych pakietu: [MLASdaneAdm::zawodyTypySzkol])
#' @details
#' Tabela *pośrednia* **P6** - jeśli jest elementem przekazanej listy - zostanie
#' zwrócona **niezmodyfikowana**.
#' @return lista ramek danych lub ramka danych - w zależności od tego, jaki
#' obiekt został przekazany argumentem `x`
#' @importFrom dplyr %>% anti_join any_of arrange count desc filter semi_join
#' @export
usun_nielegalne_zawody <- function(x, p4 = NULL,
                                   zawodyTypySzkol = MLASdaneAdm::zawodyTypySzkol) {

  stopifnot(is.data.frame(zawodyTypySzkol),
            "kod_zaw" %in% names(zawodyTypySzkol),
            "typ_szk" %in% names(zawodyTypySzkol))
  if (is.list(x) && !is.data.frame(x)) {
    stopifnot("Jeśli argumentem `x` przekazywana jest lista, argument `p4` powinien przyjąć wartość `NULL`." =
                is.null(p4),
              "Lista przekazana argumentem `x` musi zawierać element o nazwie 'p4'." =
                p4 %in% tolower(names(x)),
              "Wszystkie elementy listy przekazanej argumentem `x` muszą być ramkami danych." =
                all(sapply(x, is.data.frame)),
              "Element 'p4' listy przekazanej argumentem `x` musi zawierać kolumny 'id_abs', 'rok_abs', 'typ_szk' i 'kod_zaw'." =
                all(c("id_abs", "rok_abs", "typ_szk", "kod_zaw") %in% names(x$p4)),
              "Kolumny 'id_abs', 'rok_abs' i 'typ_szk' w elemencie 'p4' listy przekazanej argumentem `x` nie mogą zawierać braków danych." =
                !anyNA(x$p4[, c("id_abs", "rok_abs", "typ_szk")]),
              "Wszystkie elementy listy listy przekazanej argumentem `x`, z wyjątkiem `p6`, muszą zawierać kolumny 'id_abs' i 'rok_abs'." =
                all(sapply(x[names(x) != "p6"],
                           function(x) {return(all(c("id_abs", "rok_abs") %in%
                                                     names(x)))})),
              "Wszystkie elementy listy przekazanej argumentem `x`, z wyjątkiem `p6`,  nie mogą zawierać braków danych w kolumnach 'id_abs' ani 'rok_abs'." =
                all(sapply(x[names(x) != "p6"],
                           function(x) {return(!anyNA(x[ c("id_abs", "rok_abs")]))})))
    xLista <- TRUE
    bezP4 <- FALSE
    p4 <- x$p4
  } else {
    xLista <- FALSE
    if (is.null(p4)) {
      p4 <- x
      stopifnot(is.data.frame(x), is.data.frame(p4),
                all(c("typ_szk", "kod_zaw") %in% names(p4)),
                !anyNA(p4[, c("typ_szk")]))
      bezP4 <- TRUE
    } else {
      stopifnot(is.data.frame(x), is.data.frame(p4),
                all(c("id_abs", "rok_abs", "typ_szk", "kod_zaw") %in% names(p4)),
                !anyNA(p4[, c("id_abs", "rok_abs", "typ_szk")]),
                !anyNA(x[, c("id_abs", "rok_abs")]))
      bezP4 <- FALSE
    }
    x <- list(x = x)
  }

  zawodyTypySzkolP4 <- p4 %>%
    filter(!is.na(.data$kod_zaw),
           !(.data$kod_zaw %in% c(10101L, 10102L))) %>%
    count(across(any_of(c("rok_abs", "kod_zaw", "nazwa_zaw", "typ_szk"))),
          name = "lRekordow")
  if (is.factor(zawodyTypySzkolP4$typ_szk)) {
    stopifnot("Jeśli kolumna `typ_szk` w p4 jest czynnikiem, wszystkie wartości kolumny `typ_szk` w ramce danych przekazanej argumentem `zawodyTypySzkol` muszą występować wśród poziomów tego czynnika." =
                all(unique(zawodyTypySzkol$typ_szk) %in% levels(zawodyTypySzkolP4$typ_szk)),
              "Kolumny `kod_zaw` w p4 i ramce danych przekazanej argumentem `zawodyTypySzkol` muszą być tego samego typu." =
                mode(zawodyTypySzkolP4$kod_zaw) == mode(zawodyTypySzkol$kod_zaw) &&
                is.factor(zawodyTypySzkolP4$kod_zaw) == is.factor(zawodyTypySzkol$kod_zaw))
    zawodyTypySzkol$typ_szk <- factor(zawodyTypySzkol$typ_szk,
                                      levels(zawodyTypySzkolP4$typ_szk))
  }
  nielegalne <- zawodyTypySzkolP4 %>%
    anti_join(zawodyTypySzkol,
              by = c("typ_szk", "kod_zaw")) %>%
    arrange(desc(.data$lRekordow))
  if (nrow(nielegalne) > 0L) {
    message("Znaleziono rekordy opisujące absolwentów w zawodach, których nie można uczyć w danym typie szkoły:")
    print(nielegalne, row.names = FALSE)
    message("Zostaną one usunięte.")

    lRekordow <- nrow(p4)
    p4 <- p4 %>%
      anti_join(nielegalne,
                by = c("typ_szk", "kod_zaw"))
    if (bezP4) {
      x$x <- p4
    } else {
      x[names(x) != "p6"] <- lapply(x[names(x) != "p6"],
                                    semi_join, y = p4[, c("id_abs", "rok_abs")],
                                    by = c("id_abs", "rok_abs"))
    }
    message("Usunięto ", lRekordow - nrow(p4),
            ifelse(bezP4, " rekordów", " absolwentów."))
  }

  if (!xLista) {
    x <- x$x
  }
  return(x)
}
