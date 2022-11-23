arg = ARGS[1]
job_name = ENV["JADE_JOB_NAME"]
println("running job=$job_name with arg=$arg")
output_dir = joinpath(ENV["JADE_RUNTIME_OUTPUT"], "job-outputs", job_name)
mkpath(output_dir)
if arg == "3"
    error("this job failed")
end
output_file = joinpath(output_dir, "file.txt")
open(output_file, "w") do io
    write(io, arg)
    write(io, "\n")
end
