class SimpleExecutor
  def exec(cmd)
    puts(cmd)
    if ! system(cmd) then
      raise RuntimeError, "Command failed."
    end
  end
end
