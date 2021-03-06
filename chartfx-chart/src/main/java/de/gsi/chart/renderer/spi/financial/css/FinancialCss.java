package de.gsi.chart.renderer.spi.financial.css;

public class FinancialCss { // NOPMD decide not to rename it for the time being

    // Common ----------------------------------------------------------------

    /**
     * The line width  which is used for painting base shadow of the dataset
     */
    public static final String DATASET_SHADOW_LINE_WIDTH = "shadowLineWidth";

    /**
     * Transposition of original object to paint shadowed object in percent
     */
    public static final String DATASET_SHADOW_TRANSPOSITION_PERCENT = "shadowTransPercent";

    // Candlesticks ----------------------------------------------------------

    /**
     * The candle color for candle's upstick
     */
    public static final String DATASET_CANDLESTICK_LONG_COLOR = "candleLongColor";

    /**
     * The candle color for candle's downstick
     */
    public static final String DATASET_CANDLESTICK_SHORT_COLOR = "candleShortColor";

    /**
     * The candle wicks color for candle's upstick
     */
    public static final String DATASET_CANDLESTICK_LONG_WICK_COLOR = "candleLongWickColor";

    /**
     * The candle wicks color for candle's downstick
     */
    public static final String DATASET_CANDLESTICK_SHORT_WICK_COLOR = "candleShortWickColor";

    /**
     * If available, generated candlestick shadow with this defined color and transparency
     */
    public static final String DATASET_CANDLESTICK_SHADOW_COLOR = "candleShadowColor";

    /**
     * Volume Long bars with this defined color and transparency, if paintVolume=true, the volume bars are painted.
     */
    public static final String DATASET_CANDLESTICK_VOLUME_LONG_COLOR = "candleVolumeLongColor";

    /**
     * Volume Short bars with this defined color and transparency, if paintVolume=true, the volume bars are painted.
     */
    public static final String DATASET_CANDLESTICK_VOLUME_SHORT_COLOR = "candleVolumeShortColor";

    /**
     * Candle/bar relative width against actual scaled view. Defined in percentage range: {@literal <}0.0, 1.0{@literal >}
     */
    public static final String DATASET_CANDLESTICK_BAR_WIDTH_PERCENTAGE = "barWidthPercent";

    // HiLow (OHLC) ----------------------------------------------------------

    /**
     * The ohlc body color for OHLC's upstick
     */
    public static final String DATASET_HILOW_BODY_LONG_COLOR = "highLowLongColor";

    /**
     * The ohlc body color for OHLC's downstick
     */
    public static final String DATASET_HILOW_BODY_SHORT_COLOR = "highLowShortColor";

    /**
     * The ohlc body stroke for OHLC's
     */
    public static final String DATASET_HILOW_BODY_LINEWIDTH = "highLowBodyLineWidth";

    /**
     * The ohlc color for OHLC's open/close ticks
     */
    public static final String DATASET_HILOW_TICK_LONG_COLOR = "highLowLongTickColor";

    /**
     * The ohlc color for OHLC's open/close ticks
     */
    public static final String DATASET_HILOW_TICK_SHORT_COLOR = "highLowShortTickColor";

    /**
     * Volume Long bars with this defined color and transparency, if paintVolume=true, the volume bars are painted.
     */
    public static final String DATASET_HILOW_VOLUME_LONG_COLOR = "highLowVolumeLongColor";

    /**
     * Volume Short bars with this defined color and transparency, if paintVolume=true, the volume bars are painted.
     */
    public static final String DATASET_HILOW_VOLUME_SHORT_COLOR = "highLowVolumeShortColor";

    /**
     * The ohlc open/close tick stroke for OHLC's
     */
    public static final String DATASET_HILOW_TICK_LINEWIDTH = "highLowTickLineWidth";

    /**
     * If available, generated HiLow OHLC shadow with this defined color and transparency
     */
    public static final String DATASET_HILOW_SHADOW_COLOR = "hiLowShadowColor";

    /**
     * HiLow (OHLC) relative width against actual scaled view. Defined in percentage range: {@literal <}0.0, 1.0{@literal >}
     */
    public static final String DATASET_HILOW_BAR_WIDTH_PERCENTAGE = "hiLowBarWidthPercent";

    private FinancialCss() {
    }
}
