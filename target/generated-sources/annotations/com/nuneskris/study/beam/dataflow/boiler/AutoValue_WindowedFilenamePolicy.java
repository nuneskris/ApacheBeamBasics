package com.google.cloud.teleport.io;

import javax.annotation.Generated;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.ValueProvider;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_WindowedFilenamePolicy extends WindowedFilenamePolicy {

  private final ValueProvider<String> outputDirectory;

  private final ValueProvider<String> outputFilenamePrefix;

  private final ValueProvider<String> shardTemplate;

  private final ValueProvider<String> suffix;

  private final ValueProvider<String> yearPattern;

  private final ValueProvider<String> monthPattern;

  private final ValueProvider<String> dayPattern;

  private final ValueProvider<String> hourPattern;

  private final ValueProvider<String> minutePattern;

  private AutoValue_WindowedFilenamePolicy(
      @Nullable ValueProvider<String> outputDirectory,
      @Nullable ValueProvider<String> outputFilenamePrefix,
      @Nullable ValueProvider<String> shardTemplate,
      @Nullable ValueProvider<String> suffix,
      @Nullable ValueProvider<String> yearPattern,
      @Nullable ValueProvider<String> monthPattern,
      @Nullable ValueProvider<String> dayPattern,
      @Nullable ValueProvider<String> hourPattern,
      @Nullable ValueProvider<String> minutePattern) {
    this.outputDirectory = outputDirectory;
    this.outputFilenamePrefix = outputFilenamePrefix;
    this.shardTemplate = shardTemplate;
    this.suffix = suffix;
    this.yearPattern = yearPattern;
    this.monthPattern = monthPattern;
    this.dayPattern = dayPattern;
    this.hourPattern = hourPattern;
    this.minutePattern = minutePattern;
  }

  @Nullable
  @Override
  ValueProvider<String> outputDirectory() {
    return outputDirectory;
  }

  @Nullable
  @Override
  ValueProvider<String> outputFilenamePrefix() {
    return outputFilenamePrefix;
  }

  @Nullable
  @Override
  ValueProvider<String> shardTemplate() {
    return shardTemplate;
  }

  @Nullable
  @Override
  ValueProvider<String> suffix() {
    return suffix;
  }

  @Nullable
  @Override
  ValueProvider<String> yearPattern() {
    return yearPattern;
  }

  @Nullable
  @Override
  ValueProvider<String> monthPattern() {
    return monthPattern;
  }

  @Nullable
  @Override
  ValueProvider<String> dayPattern() {
    return dayPattern;
  }

  @Nullable
  @Override
  ValueProvider<String> hourPattern() {
    return hourPattern;
  }

  @Nullable
  @Override
  ValueProvider<String> minutePattern() {
    return minutePattern;
  }

  @Override
  public String toString() {
    return "WindowedFilenamePolicy{"
        + "outputDirectory=" + outputDirectory + ", "
        + "outputFilenamePrefix=" + outputFilenamePrefix + ", "
        + "shardTemplate=" + shardTemplate + ", "
        + "suffix=" + suffix + ", "
        + "yearPattern=" + yearPattern + ", "
        + "monthPattern=" + monthPattern + ", "
        + "dayPattern=" + dayPattern + ", "
        + "hourPattern=" + hourPattern + ", "
        + "minutePattern=" + minutePattern
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof WindowedFilenamePolicy) {
      WindowedFilenamePolicy that = (WindowedFilenamePolicy) o;
      return (this.outputDirectory == null ? that.outputDirectory() == null : this.outputDirectory.equals(that.outputDirectory()))
          && (this.outputFilenamePrefix == null ? that.outputFilenamePrefix() == null : this.outputFilenamePrefix.equals(that.outputFilenamePrefix()))
          && (this.shardTemplate == null ? that.shardTemplate() == null : this.shardTemplate.equals(that.shardTemplate()))
          && (this.suffix == null ? that.suffix() == null : this.suffix.equals(that.suffix()))
          && (this.yearPattern == null ? that.yearPattern() == null : this.yearPattern.equals(that.yearPattern()))
          && (this.monthPattern == null ? that.monthPattern() == null : this.monthPattern.equals(that.monthPattern()))
          && (this.dayPattern == null ? that.dayPattern() == null : this.dayPattern.equals(that.dayPattern()))
          && (this.hourPattern == null ? that.hourPattern() == null : this.hourPattern.equals(that.hourPattern()))
          && (this.minutePattern == null ? that.minutePattern() == null : this.minutePattern.equals(that.minutePattern()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (outputDirectory == null) ? 0 : outputDirectory.hashCode();
    h$ *= 1000003;
    h$ ^= (outputFilenamePrefix == null) ? 0 : outputFilenamePrefix.hashCode();
    h$ *= 1000003;
    h$ ^= (shardTemplate == null) ? 0 : shardTemplate.hashCode();
    h$ *= 1000003;
    h$ ^= (suffix == null) ? 0 : suffix.hashCode();
    h$ *= 1000003;
    h$ ^= (yearPattern == null) ? 0 : yearPattern.hashCode();
    h$ *= 1000003;
    h$ ^= (monthPattern == null) ? 0 : monthPattern.hashCode();
    h$ *= 1000003;
    h$ ^= (dayPattern == null) ? 0 : dayPattern.hashCode();
    h$ *= 1000003;
    h$ ^= (hourPattern == null) ? 0 : hourPattern.hashCode();
    h$ *= 1000003;
    h$ ^= (minutePattern == null) ? 0 : minutePattern.hashCode();
    return h$;
  }

  @Override
  WindowedFilenamePolicy.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends WindowedFilenamePolicy.Builder {
    private ValueProvider<String> outputDirectory;
    private ValueProvider<String> outputFilenamePrefix;
    private ValueProvider<String> shardTemplate;
    private ValueProvider<String> suffix;
    private ValueProvider<String> yearPattern;
    private ValueProvider<String> monthPattern;
    private ValueProvider<String> dayPattern;
    private ValueProvider<String> hourPattern;
    private ValueProvider<String> minutePattern;
    Builder() {
    }
    private Builder(WindowedFilenamePolicy source) {
      this.outputDirectory = source.outputDirectory();
      this.outputFilenamePrefix = source.outputFilenamePrefix();
      this.shardTemplate = source.shardTemplate();
      this.suffix = source.suffix();
      this.yearPattern = source.yearPattern();
      this.monthPattern = source.monthPattern();
      this.dayPattern = source.dayPattern();
      this.hourPattern = source.hourPattern();
      this.minutePattern = source.minutePattern();
    }
    @Override
    WindowedFilenamePolicy.Builder setOutputDirectory(ValueProvider<String> outputDirectory) {
      this.outputDirectory = outputDirectory;
      return this;
    }
    @Override
    WindowedFilenamePolicy.Builder setOutputFilenamePrefix(ValueProvider<String> outputFilenamePrefix) {
      this.outputFilenamePrefix = outputFilenamePrefix;
      return this;
    }
    @Override
    WindowedFilenamePolicy.Builder setShardTemplate(ValueProvider<String> shardTemplate) {
      this.shardTemplate = shardTemplate;
      return this;
    }
    @Override
    WindowedFilenamePolicy.Builder setSuffix(ValueProvider<String> suffix) {
      this.suffix = suffix;
      return this;
    }
    @Override
    WindowedFilenamePolicy.Builder setYearPattern(ValueProvider<String> yearPattern) {
      this.yearPattern = yearPattern;
      return this;
    }
    @Override
    WindowedFilenamePolicy.Builder setMonthPattern(ValueProvider<String> monthPattern) {
      this.monthPattern = monthPattern;
      return this;
    }
    @Override
    WindowedFilenamePolicy.Builder setDayPattern(ValueProvider<String> dayPattern) {
      this.dayPattern = dayPattern;
      return this;
    }
    @Override
    WindowedFilenamePolicy.Builder setHourPattern(ValueProvider<String> hourPattern) {
      this.hourPattern = hourPattern;
      return this;
    }
    @Override
    WindowedFilenamePolicy.Builder setMinutePattern(ValueProvider<String> minutePattern) {
      this.minutePattern = minutePattern;
      return this;
    }
    @Override
    WindowedFilenamePolicy build() {
      return new AutoValue_WindowedFilenamePolicy(
          this.outputDirectory,
          this.outputFilenamePrefix,
          this.shardTemplate,
          this.suffix,
          this.yearPattern,
          this.monthPattern,
          this.dayPattern,
          this.hourPattern,
          this.minutePattern);
    }
  }

}
