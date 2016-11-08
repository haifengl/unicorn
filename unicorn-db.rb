class UnicornDb < Formula
  desc "BigTable, Document, Graph and Full Text Search Database"
  homepage "https://github.com/haifengl/unicorn/"
  url "https://github.com/haifengl/unicorn/releases/download/v2.1.1/unicorn-2.1.1.tgz"
  sha256 "de9f550f18f4123edb35a5e944d5b4ee55ce796351170a62bfa9d80a13d1f69a"

  bottle :unneeded

  depends_on :java => "1.8+"

  def install
    libexec.install Dir["*"]
    bin.write_exec_script libexec/"bin/unicorn"
  end

  test do
    system "#{bin}/unicorn", "help"
  end
end
