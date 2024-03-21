# 📓 Notebooks

The notebooks in this directory largely analyse results for the three measures of greenness: industries, occupations and skills. The files used across the analyses are defined in `ojo_analysis.yaml`.

## 👠 High level analysis

The below notebooks contain high-level analysis of the green measures. They also contain a a notebook that analyses both a random sample of 1000000 of job adverts and a weighted sample of 1000000 of job adverts as defined in `dap_prinz_green_jobs/pipeline/ojo_application/sample_ojo.py`.

### Between Measures Analysis

The `between_measures_analysis.ipynb` notebook contains analysis of green measures **between** industries, occupations and skills.

### Measures Analysis

This notebook contains high-level analysis of industries, occupations and skills green measures.

### Sampling Analysis

This notebook contains high-level analysis of different OJO samples.

## 🥬 Green Jobs Explorer Analysis

The below notebooks create graphs that are used in the Green Jobs Explorer.

### Regional Comparison

The `regional_comparison_new.ipynb` notebook contains code to create a chloropleth of regional comparisons of the greenness measures for the Green Jobs Explorer.

### 2x2 Typologies

The `2x2_typologies.ipynb` notebook contains code to create a 2x2 typology of greenness measures for the comparisons flow of the Green Jobs Explorer.

### New Skills analysis

The `new_skills_analysis.ipynb` notebook contains code to create graphs that explore new 'green' skills for the comparisons flow of the Green Jobs Explorer.

### Violin plots

The `violin_plots.ipynb` notebook contains code to create violin plots of the greenness measures for the the Green Jobs Explorer.

### Common skills plots

The `common_skills.ipynb` notebook contains code to create bar plots of the most common skills and green skills.

### Similar occupations plots

The `Similar_occupations.ipynb` notebook contains code to create the bar plots of similar occupations for a single occupation selected from a drop down.

### How to make tootlips have a maximum width.

After creating the html plots for the Green Jobs Explorer using the above notebooks, you will need to manually edit all the html's produced in Altair. This is to make sure the tooltip widths are slim enough to not overlap the edge of the plot (which happens by default).

You add two things:

```
<!DOCTYPE html>
<html>
<head>
  <style>
    #vis.vega-embed {
      width: 100%;
      display: flex;
    }

    #vis.vega-embed details,
    #vis.vega-embed details summary {
      position: relative;
    }
    #vg-tooltip-element.vg-tooltip.custom-theme {
      max-width:  40%;
    }
  </style>
```

i.e. add the

```
#vg-tooltip-element.vg-tooltip.custom-theme {
      max-width:  40%;
    }
```

to the style. You can change max-width to be a % or a value in px (e.g. 200px).

And:

```
var tooltipOptions = {
  theme: 'custom'
};
var embedOpt = {"mode": "vega-lite", tooltip: tooltipOptions};

```

at the end, instead of just

```
var embedOpt = {"mode": "vega-lite"};


```

### Add dropdown above plot

By default Altair puts the dropdown selector box to the bottom left of the plot, to change this, add:

```
form.vega-bindings {
      position: absolute;
      left: 0px;
      top: -10px;
    }

```

in the `<style>` part of the htmls.

You may wish to leave a gap for this between the chart title and the plot - if so when you save the plot in your python code, you can add a gap:

```
fig.configure_title(offset=100).save("plot.html")
```
