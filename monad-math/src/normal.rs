// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

//! The standard normal distribution: density, distribution function, upper
//! tail, quantile function and inverse Mills ratio.

use core::f64::consts::SQRT_2;

// 1 / sqrt(2π)
const FRAC_1_SQRT_2PI: f64 = 0.398_942_280_401_432_7;

// Beyond this argument the inverse Mills ratio uses its asymptotic series:
// the series is then exact to double precision, and the direct ratio would
// divide two numbers that are about to underflow.
const INV_MILLS_SERIES_FROM: f64 = 30.0;

/// The density `φ(x) = exp(−x²/2) / √(2π)`.
///
/// Underflows to 0 for `|x|` above about 38.6.
pub fn pdf(x: f64) -> f64 {
    FRAC_1_SQRT_2PI * libm::exp(-0.5 * x * x)
}

/// The distribution function `Φ(x) = P[X ≤ x]`.
///
/// Computed as `erfc(−x/√2) / 2`, so the lower tail keeps its relative
/// accuracy: `cdf(-10.0)` is about 7.6e-24, not 0. For the upper tail use
/// [`sf`]; `1.0 - cdf(x)` loses it.
pub fn cdf(x: f64) -> f64 {
    0.5 * libm::erfc(-x / SQRT_2)
}

/// The upper tail `1 − Φ(x) = P[X > x]`, with its relative accuracy kept:
/// `sf(10.0)` is about 7.6e-24. `sf(x) == cdf(-x)` exactly.
pub fn sf(x: f64) -> f64 {
    0.5 * libm::erfc(x / SQRT_2)
}

/// The quantile function `Φ⁻¹(p)`: the `x` with `cdf(x) == p`.
///
/// Wichura's algorithm AS 241 (PPND16), accurate to about 1 part in 10¹⁶.
/// `ppf(0.0)` is −∞ and `ppf(1.0)` is +∞; outside `[0, 1]` and for NaN the
/// result is NaN.
///
/// For a tail probability `q` close to 0, `ppf(1.0 - q)` has already lost
/// the digits of `q` in the subtraction; use `-ppf(q)`.
pub fn ppf(p: f64) -> f64 {
    if p.is_nan() || !(0.0..=1.0).contains(&p) {
        return f64::NAN;
    }
    if p == 0.0 {
        return f64::NEG_INFINITY;
    }
    if p == 1.0 {
        return f64::INFINITY;
    }

    let q = p - 0.5;
    if q.abs() <= 0.425 {
        let r = 0.180625 - q * q;
        let num = (((((((2.509_080_928_730_122_7e3 * r + 3.343_057_558_358_813e4) * r
            + 6.726_577_092_700_87e4)
            * r
            + 4.592_195_393_154_987e4)
            * r
            + 1.373_169_376_550_946_2e4)
            * r
            + 1.971_590_950_306_551_3e3)
            * r
            + 1.331_416_678_917_843_8e2)
            * r
            + 3.387_132_872_796_366_5)
            * q;
        let den = ((((((5.226_495_278_852_854e3 * r + 2.872_908_573_572_194_3e4) * r
            + 3.930_789_580_009_271e4)
            * r
            + 2.121_379_430_158_659_7e4)
            * r
            + 5.394_196_021_424_751e3)
            * r
            + 6.871_870_074_920_579e2)
            * r
            + 4.231_333_070_160_091e1)
            * r
            + 1.0;
        return num / den;
    }

    // the tail: r = sqrt(−ln(min(p, 1 − p)))
    let tail = if q < 0.0 { p } else { 1.0 - p };
    let r = libm::sqrt(-libm::log(tail));
    let x = if r <= 5.0 {
        let r = r - 1.6;
        let num = ((((((7.745_450_142_783_414e-4 * r + 2.272_384_498_926_918_4e-2) * r
            + 2.417_807_251_774_506e-1)
            * r
            + 1.270_458_252_452_368_4)
            * r
            + 3.647_848_324_763_204_5)
            * r
            + 5.769_497_221_460_691)
            * r
            + 4.630_337_846_156_546)
            * r
            + 1.423_437_110_749_683_5;
        let den = ((((((1.050_750_071_644_416_9e-9 * r + 5.475_938_084_995_345e-4) * r
            + 1.519_866_656_361_645_7e-2)
            * r
            + 1.481_039_764_274_800_8e-1)
            * r
            + 6.897_673_349_851e-1)
            * r
            + 1.676_384_830_183_803_8)
            * r
            + 2.053_191_626_637_759)
            * r
            + 1.0;
        num / den
    } else {
        let r = r - 5.0;
        let num = ((((((2.010_334_399_292_288_1e-7 * r + 2.711_555_568_743_487_6e-5) * r
            + 1.242_660_947_388_078_4e-3)
            * r
            + 2.653_218_952_657_612_4e-2)
            * r
            + 2.965_605_718_285_048_7e-1)
            * r
            + 1.784_826_539_917_291_3)
            * r
            + 5.463_784_911_164_114)
            * r
            + 6.657_904_643_501_103;
        let den = ((((((2.044_263_103_389_939_7e-15 * r + 1.421_511_758_316_446e-7) * r
            + 1.846_318_317_510_054_8e-5)
            * r
            + 7.868_691_311_456_133e-4)
            * r
            + 1.487_536_129_085_061_5e-2)
            * r
            + 1.369_298_809_227_358e-1)
            * r
            + 5.998_322_065_558_88e-1)
            * r
            + 1.0;
        num / den
    };
    if q < 0.0 {
        -x
    } else {
        x
    }
}

/// The inverse Mills ratio `λ(x) = φ(x) / (1 − Φ(x))`: the mean of a
/// standard normal variable given that it exceeds `x`, and the derivative
/// that a probit likelihood needs.
///
/// Stable over the whole line. It tends to 0 as `x → −∞`, is about 0.798 at
/// 0, and approaches `x + 1/x` from below as `x → ∞`. Far out, numerator
/// and denominator both underflow, so from 30 on the asymptotic series
/// `x + 1/x − 2/x³ + 10/x⁵ − 74/x⁷ + 706/x⁹` is used; its error there is
/// below 1 part in 10¹³.
pub fn inv_mills(x: f64) -> f64 {
    if x >= INV_MILLS_SERIES_FROM {
        let t = 1.0 / x;
        let t2 = t * t;
        return x + t * (1.0 + t2 * (-2.0 + t2 * (10.0 + t2 * (-74.0 + 706.0 * t2))));
    }
    pdf(x) / sf(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close_rel(actual: f64, expected: f64, tolerance: f64) {
        let scale = expected.abs().max(f64::MIN_POSITIVE);
        assert!(
            (actual - expected).abs() <= tolerance * scale,
            "{actual:e} != {expected:e} (relative {tolerance:e})"
        );
    }

    fn close_abs(actual: f64, expected: f64, tolerance: f64) {
        assert!(
            (actual - expected).abs() <= tolerance,
            "{actual:e} != {expected:e} (± {tolerance:e})"
        );
    }

    // References from the Python 3 standard library, printed with repr().
    // (p, quantile): statistics.NormalDist().inv_cdf(p), also AS 241.
    const PPF: [(f64, f64); 15] = include!("../testdata/ppf.in");
    // (x, upper tail, density): 0.5 * math.erfc(x / math.sqrt(2)) and
    // math.exp(-x * x / 2) / math.sqrt(2 * math.pi).
    const SF_PDF: [(f64, f64, f64); 13] = include!("../testdata/sf_pdf.in");
    // (x, ratio): for x > 0 Laplace's continued fraction
    // x + 1/(x + 2/(x + 3/(x + ...))) in 60-digit decimals; for x <= 0 the
    // ratio of the two expressions above.
    const INV_MILLS: [(f64, f64); 12] = include!("../testdata/inv_mills.in");

    #[test]
    fn ppf_matches_the_references() {
        for (p, expected) in PPF {
            if expected.abs() < 1.0 {
                close_abs(ppf(p), expected, 1e-13);
            } else {
                close_rel(ppf(p), expected, 1e-13);
            }
        }
    }

    // the planner's default margin, z for a risk of 1 in 1000
    #[test]
    fn ppf_of_a_tail_of_one_in_a_thousand() {
        close_abs(-ppf(1e-3), 3.090_232_306_167_813, 1e-12);
        close_abs(ppf(1.0 - 1e-3), 3.090_232_306_167_813, 1e-12);
    }

    #[test]
    fn ppf_at_the_edges() {
        assert_eq!(ppf(0.0), f64::NEG_INFINITY);
        assert_eq!(ppf(1.0), f64::INFINITY);
        assert_eq!(ppf(0.5), 0.0);
        assert!(ppf(-0.1).is_nan());
        assert!(ppf(1.1).is_nan());
        assert!(ppf(f64::NAN).is_nan());
        // the smallest positive number still has a finite quantile
        assert!(ppf(f64::MIN_POSITIVE).is_finite());
    }

    #[test]
    fn sf_cdf_and_pdf_match_the_references() {
        for (x, tail, density) in SF_PDF {
            close_rel(sf(x), tail, 1e-13);
            close_rel(cdf(-x), tail, 1e-13);
            close_rel(pdf(x), density, 1e-14);
        }
    }

    #[test]
    fn the_two_tails_mirror_each_other() {
        let mut x = -40.0;
        while x <= 40.0 {
            assert_eq!(cdf(-x), sf(x), "x = {x}");
            close_abs(cdf(x) + sf(x), 1.0, 1e-15);
            x += 0.37;
        }
    }

    #[test]
    fn ppf_inverts_cdf() {
        // through the tail that keeps its accuracy: cdf below 0, sf above
        let mut x = -30.0;
        while x <= 0.0 {
            close_abs(ppf(cdf(x)), x, 1e-12 * x.abs().max(1.0));
            close_abs(-ppf(sf(-x)), -x, 1e-12 * x.abs().max(1.0));
            x += 0.173;
        }
        for k in 1..1000 {
            let p = f64::from(k) / 1000.0;
            close_rel(cdf(ppf(p)), p, 1e-14);
        }
        for exponent in 1..300 {
            let p = libm::pow(10.0, -f64::from(exponent));
            close_rel(cdf(ppf(p)), p, 1e-12);
        }
    }

    #[test]
    fn ppf_is_odd_around_one_half() {
        for k in 1..500 {
            let p = f64::from(k) / 1024.0;
            assert_eq!(ppf(p), -ppf(1.0 - p), "p = {p}");
        }
    }

    #[test]
    fn everything_is_monotone() {
        let mut previous = (f64::NEG_INFINITY, 0.0, 0.0);
        let mut x = -38.0;
        while x <= 38.0 {
            let current = (ppf(cdf(x)), cdf(x), inv_mills(x));
            assert!(current.0 >= previous.0, "ppf at {x}");
            assert!(current.1 >= previous.1, "cdf at {x}");
            assert!(current.2 > previous.2, "inv_mills at {x}");
            previous = current;
            x += 0.011;
        }
        let mut previous = f64::NEG_INFINITY;
        for k in 1..10_000 {
            let quantile = ppf(f64::from(k) / 10_000.0);
            assert!(quantile > previous);
            previous = quantile;
        }
    }

    #[test]
    fn inv_mills_matches_the_references() {
        for (x, expected) in INV_MILLS {
            close_rel(inv_mills(x), expected, 1e-12);
        }
    }

    // the direct ratio and the series agree where one hands over to the other
    #[test]
    fn inv_mills_is_continuous_at_the_switch() {
        let at = INV_MILLS_SERIES_FROM;
        close_rel(inv_mills(at), pdf(at) / sf(at), 1e-12);
        // and the series is as good a little below the switch
        let below = at - 2.0;
        let t = 1.0 / below;
        let t2 = t * t;
        let series = below + t * (1.0 + t2 * (-2.0 + t2 * (10.0 + t2 * (-74.0 + 706.0 * t2))));
        close_rel(inv_mills(below), series, 1e-12);
    }

    #[test]
    fn inv_mills_is_finite_and_above_its_argument() {
        let mut x = -38.0;
        while x <= 1e6 {
            let ratio = inv_mills(x);
            assert!(ratio.is_finite() && ratio > 0.0, "x = {x}: {ratio}");
            if x > 0.0 {
                // far out the gap to x + 1/x is below one unit in the last place
                assert!(ratio > x, "x = {x}: {ratio}");
                assert!(ratio <= x + 1.0 / x, "x = {x}: {ratio}");
            }
            x = if x < 40.0 { x + 0.25 } else { x * 1.5 };
        }
    }
}
