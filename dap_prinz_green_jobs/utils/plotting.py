"""
Functions to make altair graphs look nice
"""

import altair as alt
import pandas as pd

GREEN_MEASURES_COLORS = ["#1a9641", "#d8b365", "#8c510a"]  # green  # neutral  # brown

NESTA_COLOURS = [
    #    "#0000FF",
    "#FDB633",
    "#18A48C",
    "#9A1BBE",
    "#EB003B",
    "#FF6E47",
    "#646363",
    "#0F294A",
    "#97D9E3",
    "#A59BEE",
    "#F6A4B7",
    "#D2C9C0",
    "#000000",
]

# New brand colours
NESTA_COLOURS_DICT = {
    "green": "#86efad",
    "blue": "#97d9e3",
    "purple": "#a59bee",
    "red": "#eb003b",
    "yellow": "#fdb633",
    "pink": "#f6a4b7",
}


def nestafont(font: str = "Century Gothic"):
    """Define Nesta fonts"""
    return {
        "config": {
            "title": {"font": font, "anchor": "start"},
            "axis": {"labelFont": font, "titleFont": font},
            "header": {"labelFont": font, "titleFont": font},
            "legend": {"labelFont": font, "titleFont": font},
            "range": {
                "category": NESTA_COLOURS,
                "ordinal": {
                    "scheme": NESTA_COLOURS
                },  # this will interpolate the colors
            },
        }
    }


alt.themes.register("nestafont", nestafont)
alt.themes.enable("nestafont")


def configure_plots(
    fig,
    font: str = "Century Gothic",
    chart_title: str = "",
    chart_subtitle: str = "",
    fontsize_title: int = 16,
    fontsize_subtitle: int = 13,
    fontsize_normal: int = 13,
):
    """Adds titles, subtitles; configures font sizes; adjusts gridlines"""
    return (
        fig.properties(
            title={
                "anchor": "start",
                "text": chart_title,
                "fontSize": fontsize_title,
                "subtitle": chart_subtitle,
                "subtitleFont": font,
                "subtitleFontSize": fontsize_subtitle,
            },
        )
        .configure_axis(
            gridDash=[1, 7],
            gridColor="grey",
            labelFontSize=fontsize_normal,
            titleFontSize=fontsize_normal,
        )
        .configure_legend(
            titleFontSize=fontsize_title,
            labelFontSize=fontsize_normal,
        )
        .configure_view(strokeWidth=0)
    )
