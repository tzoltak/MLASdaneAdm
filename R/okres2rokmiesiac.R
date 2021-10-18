#' @title Zamiana "okresu" na rok i miesiac
#' @description Funkcja pomocnicza pozwalająca zamienić numer "okresu", używany
#' jako identyfikator miesiąca w tabelach \emph{pośrednich} na odpowiadający mu
#' rok i numer miesiąca (w ramach roku).
#' @param x liczba całkowita (co do zasady wartość kolumny `okres`)
#' @return dwuelementowy wektor liczb całkowitych, z których pierwsza opisuje
#' rok, a druga numer miesiąca (w ramach roku)
#' @examples okres2rokmiesiac(24249)
#' @export
okres2rokmiesiac = function(x) {
  return(c(rok = floor((x - 1) / 12), miesiac = x - 12L*(floor((x - 1) / 12))))
}
