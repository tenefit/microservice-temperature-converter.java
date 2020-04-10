/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

public final class TempUtils
{
    public enum TemperatureUnit
    {
        C, F, K
    }

    public static int convertTemp(int temp, TemperatureUnit fromUnit, TemperatureUnit toUnit)
    {
        if (fromUnit.equals(toUnit))
        {
            return temp;
        }

        double result = 0;

        switch (fromUnit)
        {
        case C:
            if (toUnit == TemperatureUnit.F)
            {
                result = celsiusToFahrenheit(temp);
            }
            else
            {
                result = celsiusToKelvin(temp);
            }
            break;
        case F:
            if (toUnit == TemperatureUnit.C)
            {
                result = fahrenheitToCelsius(temp);
            }
            else
            {
                result = fahrenheitToKelvin(temp);
            }
            break;
        case K:
            if (toUnit == TemperatureUnit.C)
            {
                result = kelvinToCelsius(temp);
            }
            else
            {
                result = kelvinToFahrenheit(temp);
            }
            break;
        }
        return (int)Math.round(result);
    }

    private TempUtils()
    {
        // utility class
    }

    private static double fahrenheitToCelsius(double temp)
    {
        return (temp - 32.0) / 9.0 * 5.0;
    }

    private static double fahrenheitToKelvin(double temp)
    {
        return (temp + 459.7) / 9.0 * 5.0;
    }

    private static double celsiusToFahrenheit(double temp)
    {
        return temp / 5.0 * 9.0 + 32.0;
    }

    private static double celsiusToKelvin(double temp)
    {
        return temp + 273.2;
    }

    private static double kelvinToCelsius(double temp)
    {
        return temp - 273.2;
    }

    private static double kelvinToFahrenheit(double temp)
    {
        return temp / 5.0 * 9.0 - 459.7;
    }
}
