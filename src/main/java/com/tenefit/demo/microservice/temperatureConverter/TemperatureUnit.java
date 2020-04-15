/**
 * Copyright 2016-2020 Tenefit. All rights reserved.
 */
package com.tenefit.demo.microservice.temperatureConverter;

public enum TemperatureUnit
{
    C
    {
        public int fromCelsius(
            int value)
        {
            return value;
        }

        public int fromFahrenheit(
            int value)
        {
            return (int)Math.round((value - 32.0) / 9.0 * 5.0);
        }

        public int fromKelvin(
            int value)
        {
            return (int)Math.round(value - 273.2);
        }

        public int convertTo(
            int value,
            TemperatureUnit unit)
        {
            return unit.fromCelsius(value);
        }
    },

    F
    {
        public int fromCelsius(
            int value)
        {
            return (int)Math.round(value / 5.0 * 9.0 + 32.0);
        }

        public int fromFahrenheit(
            int value)
        {
            return value;
        }

        public int fromKelvin(
            int value)
        {
            return (int)Math.round(value / 5.0 * 9.0 - 459.7);
        }

        public int convertTo(
            int value,
            TemperatureUnit unit)
        {
            return unit.fromFahrenheit(value);
        }
    },

    K
    {
        public int fromCelsius(
            int value)
        {
            return (int)Math.round(value + 273.2);
        }

        public int fromFahrenheit(
            int value)
        {
            return (int)Math.round((value + 459.7) / 9.0 * 5.0);
        }

        public int fromKelvin(
            int value)
        {
            return value;
        }

        public int convertTo(
            int value,
            TemperatureUnit unit)
        {
            return unit.fromKelvin(value);
        }
    };

    public abstract int fromCelsius(
        int value);

    public abstract int fromFahrenheit(
        int value);

    public abstract int fromKelvin(
        int value);

    abstract int convertTo(
        int value,
        TemperatureUnit unit);
}
